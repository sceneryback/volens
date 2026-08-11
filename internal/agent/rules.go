package agent

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/volcano-sh/volens/internal/agent/enqueue"
	"github.com/volcano-sh/volens/internal/agent/filter"
	runtimeanalysis "github.com/volcano-sh/volens/internal/agent/runtime"
	"github.com/volcano-sh/volens/internal/agent/validate"
	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
)

// Rule evaluates one ordered phase of the Volcano scheduling diagnosis.
// Implementations should append evidence to state.Report and call state.Stop
// when Volcano would not reach later rules.
type Rule interface {
	Name() string
	Evaluate(context.Context, *RuleState) error
}

// RuleState carries shared analysis data across registered rules. It keeps
// the rule interface small while avoiding repeated Kubernetes reads.
type RuleState struct {
	Agent   *Agent
	Request Request
	Report  *Report

	pod                 *corev1.Pod
	podGroupName        string
	podGroupErr         error
	scheduler           cluster.Scheduler
	schedulerErr        error
	cacheDump           cluster.CacheDump
	cacheDumpErr        error
	finishWithoutSource bool
	stopped             bool
	evidence            *presentationEvidence
}

var (
	analysisRulesMu sync.RWMutex
	analysisRules   []Rule
)

func init() {
	RegisterRule(validateRule{})
	RegisterRule(enqueueRule{})
	RegisterRule(jobValidRule{})
	RegisterRule(allocateRule{})
}

// RegisterRule appends a diagnostic rule to the global Analyze pipeline.
// Rules run in registration order, so plugins should register after the phase
// they depend on and before phases they may short-circuit.
func RegisterRule(rule Rule) {
	if rule == nil {
		panic("agent rule is nil")
	}

	analysisRulesMu.Lock()
	defer analysisRulesMu.Unlock()

	analysisRules = append(analysisRules, rule)
}

func analysisRuleSnapshot() []Rule {
	analysisRulesMu.RLock()
	defer analysisRulesMu.RUnlock()

	return append([]Rule(nil), analysisRules...)
}

func newRuleState(agent *Agent, request Request) *RuleState {
	report := &Report{Request: request}

	return &RuleState{
		Agent:    agent,
		Request:  request,
		Report:   report,
		evidence: &presentationEvidence{},
	}
}

func (s *RuleState) stopWithoutSource(evidence presentationEvidence) {
	s.evidence = &evidence
	s.finishWithoutSource = true
	s.stopped = true
}

func (s *RuleState) stopWithSource() {
	s.stopped = true
}

type validateRule struct{}

func (validateRule) Name() string {
	return "validate"
}

func (validateRule) Evaluate(ctx context.Context, state *RuleState) error {
	pod, podErr := state.Agent.clusterManager.GetPod(
		ctx,
		state.Request.Namespace,
		state.Request.Pod,
	)
	if podErr != nil {
		check := targetPodLookupCheck(state.Request, podErr)
		state.Report.Checks = append(state.Report.Checks, check)
		state.stopWithoutSource(stoppedAtPreflight(check))

		return nil
	}

	state.evidence.Pod = pod
	state.pod = pod

	scheduler, schedulerErr := state.Agent.clusterManager.GetVolcanoScheduler(ctx)
	state.scheduler = scheduler
	state.schedulerErr = schedulerErr
	state.Report.Scheduler = scheduler
	log.Printf(
		"analyze request namespace=%s pod=%s branch=%s scheduler=%s/%s schedulerErr=%v",
		state.Request.Namespace,
		state.Request.Pod,
		state.Request.Branch,
		scheduler.Namespace,
		scheduler.Name,
		schedulerErr,
	)
	state.Report.Checks = append(
		state.Report.Checks,
		validate.Evaluate(pod, scheduler, schedulerErr)...,
	)

	configuration, configurationErr := state.Agent.clusterManager.GetVolcanoSchedulerConfigurationFor(
		ctx,
		scheduler,
	)
	policy, policyChecks := runtimeanalysis.ObservePolicy(configuration, configurationErr)
	state.Report.Policy = policy
	state.Report.Checks = append(state.Report.Checks, policyChecks...)

	podGroupName := validate.PodGroupName(pod)
	group, groupErr, tasks, tasksErr := state.Agent.collectPodGroup(
		ctx,
		state.Request.Namespace,
		podGroupName,
	)
	state.podGroupName = podGroupName
	state.podGroupErr = groupErr
	state.evidence.PodGroup = group
	state.evidence.Tasks = tasks
	state.Report.Checks = append(
		state.Report.Checks,
		validate.EvaluatePodGroup(podGroupName, group, groupErr, tasks, tasksErr)...,
	)

	if failed, found := knownStageFailure(state.Report.Checks, "preflight"); found {
		evidence := stoppedAtPreflight(failed)
		evidence.Pod = pod
		evidence.Tasks = tasks
		evidence.PodGroup = group
		state.stopWithoutSource(evidence)
	}

	return nil
}

type enqueueRule struct{}

func (enqueueRule) Name() string {
	return "enqueue"
}

func (enqueueRule) Evaluate(ctx context.Context, state *RuleState) error {
	pod, ok := state.evidence.Pod, state.evidence.Pod != nil
	if !ok {
		return fmt.Errorf("validate rule did not load target Pod")
	}

	queueName, queueNameErr, queue, queueErr := state.Agent.collectQueue(
		ctx,
		state.evidence.PodGroup,
		state.podGroupErr,
		state.scheduler,
		state.schedulerErr,
	)
	podEvents, podEventsErr, groupEvents, groupEventsErr := state.Agent.collectEnqueueEvents(
		ctx,
		state.Request,
		state.podGroupName,
		state.evidence.PodGroup,
		state.podGroupErr,
	)

	state.Report.Checks = append(state.Report.Checks, enqueue.Evaluate(enqueue.Input{
		SchedulerName:      pod.Spec.SchedulerName,
		Actions:            state.Report.Policy.Actions,
		ActionsDeterminate: state.Report.Policy.Determinate,
		ActiveDeterminate:  state.Report.Policy.ActiveDeterminate,
		PodGroup:           state.evidence.PodGroup,
		PodGroupErr:        state.podGroupErr,
		QueueName:          queueName,
		QueueNameErr:       queueNameErr,
		Queue:              queue,
		QueueErr:           queueErr,
		PodEvents:          podEvents,
		PodEventsErr:       podEventsErr,
		GroupEvents:        groupEvents,
		GroupEventsErr:     groupEventsErr,
	})...)

	state.evidence.QueueName = queueName
	state.evidence.Queue = queue
	state.evidence.QueueErr = queueErr
	loadQueuePresentationEvidence(ctx, state, queueName, queueNameErr, queueErr)
	log.Printf(
		"enqueue checks namespace=%s pod=%s queue=%s checks=%d",
		state.Request.Namespace,
		state.Request.Pod,
		queueName,
		len(state.Report.Checks),
	)

	if !observedEnqueueAction(state.Report.Policy) {
		return nil
	}

	if failed, found := knownStageFailure(state.Report.Checks, "enqueue"); found {
		state.evidence.EnqueueStopped = true
		state.evidence.AllocateStopped = true
		state.evidence.StopReason = "enqueue 明确失败：" + failed.Name + " — " + failed.Reason
		state.stopWithSource()
	}

	return nil
}

func loadQueuePresentationEvidence(
	ctx context.Context,
	state *RuleState,
	queueName string,
	queueNameErr error,
	queueErr error,
) {
	if queueNameErr == nil {
		defaultQueueName := ""
		if state.schedulerErr == nil && state.scheduler.ConfigurationDeterminate {
			defaultQueueName = state.scheduler.DefaultQueue
		}

		state.evidence.QueuePodGroups, state.evidence.QueuePodGroupsErr =
			state.Agent.clusterManager.ListPodGroupsForQueue(ctx, queueName, defaultQueueName)
	} else {
		state.evidence.QueuePodGroupsErr = queueNameErr
	}

	switch {
	case state.schedulerErr != nil:
		state.evidence.QueueSnapshotErr = state.schedulerErr
	case queueNameErr != nil:
		state.evidence.QueueSnapshotErr = queueNameErr
	case queueErr != nil:
		state.evidence.QueueSnapshotErr = queueErr
	default:
		queues, queuesErr := state.Agent.clusterManager.ListQueues(ctx)
		if queuesErr != nil {
			state.evidence.QueueSnapshotErr = queuesErr

			return
		}

		dump, dumpErr := state.captureCacheDump(ctx)
		if dumpErr != nil {
			state.evidence.QueueSnapshotErr = dumpErr

			return
		}

		state.evidence.QueueSnapshot, state.evidence.QueueSnapshotErr =
			cluster.BuildQueueSnapshot(
				dump,
				queues,
				state.evidence.QueuePodGroups,
				queueName,
				queueBaseStrategy(queueStrategy(state.Report.Policy)),
			)
	}
}

func (s *RuleState) captureCacheDump(ctx context.Context) (cluster.CacheDump, error) {
	if s.cacheDump.Nodes != nil || s.cacheDumpErr != nil {
		return s.cacheDump, s.cacheDumpErr
	}

	s.cacheDump, s.cacheDumpErr = s.Agent.clusterManager.CaptureCacheDump(ctx)

	return s.cacheDump, s.cacheDumpErr
}

type jobValidRule struct{}

func (jobValidRule) Name() string {
	return "jobValid"
}

func (jobValidRule) Evaluate(_ context.Context, state *RuleState) error {
	if failed, found := knownStageFailure(state.Report.Checks, "jobValid"); found {
		state.evidence.AllocateStopped = true
		state.evidence.StopReason = "JobValid 明确失败：" + failed.Name + " — " + failed.Reason
		state.stopWithSource()
	}

	return nil
}

type allocateRule struct{}

func (allocateRule) Name() string {
	return "allocate"
}

func (allocateRule) Evaluate(ctx context.Context, state *RuleState) error {
	if state.evidence.Pod == nil {
		return fmt.Errorf("validate rule did not load target Pod")
	}

	nodes, nodesErr := state.Agent.clusterManager.ListNodes(ctx)
	dump, cacheErr := state.captureCacheDump(ctx)

	filterResult := filter.Evaluate(filter.Input{
		Pod:        state.evidence.Pod,
		Nodes:      nodes,
		NodesErr:   nodesErr,
		Dump:       dump,
		CaptureErr: cacheErr,
	})
	state.Report.Checks = append(state.Report.Checks, filterResult.CacheCheck)
	state.Report.Checks = append(
		state.Report.Checks,
		schedulerEvidenceIdentityCheck(state.Report.Policy, dump.Scheduler),
	)
	state.Report.Nodes = filterResult.Nodes
	state.Report.Checks = append(
		state.Report.Checks,
		filterResult.AllocationCheck,
		filterResult.PluginCheck,
	)

	return nil
}
