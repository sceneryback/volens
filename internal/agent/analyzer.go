package agent

import (
	"context"
	"fmt"
	"log"

	"github.com/volcano-sh/volens/internal/agent/enqueue"
	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
	"github.com/volcano-sh/volens/internal/source"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type Agent struct {
	clusterManager *cluster.Client
	sourceManager  *source.Manager
	llm            LLMConfig
}

func New(
	clusterManager *cluster.Client,
	sourceManager *source.Manager,
	llmConfig LLMConfig,
) *Agent {
	return &Agent{
		clusterManager: clusterManager,
		sourceManager:  sourceManager,
		llm:            llmConfig,
	}
}

// Analyze runs the registered scheduler rules in Volcano's scheduling order
// and then builds the user-facing report. Each rule owns one phase of evidence
// collection and may stop later phases when Volcano would not reach them.
func (a *Agent) Analyze(ctx context.Context, request Request) (Report, error) {
	if request.Namespace == "" || request.Pod == "" {
		return Report{}, fmt.Errorf("namespace and pod are required")
	}

	if a.clusterManager == nil {
		return Report{}, fmt.Errorf("cluster manager is not configured")
	}

	state := newRuleState(a, request)

	for _, rule := range analysisRuleSnapshot() {
		if err := rule.Evaluate(ctx, state); err != nil {
			return Report{}, fmt.Errorf("evaluate rule %s: %w", rule.Name(), err)
		}

		if state.stopped {
			break
		}
	}

	if state.finishWithoutSource {
		finishReport(state.Report, *state.evidence)

		return *state.Report, nil
	}

	a.completeReport(ctx, state.Report, state.scheduler, *state.evidence)

	return *state.Report, nil
}

func stoppedAtPreflight(check model.Check) presentationEvidence {
	prefix := "preflight 无法完成"

	if check.Determinate && !check.Passed && !check.Skipped {
		prefix = "preflight 明确失败"
	}

	return presentationEvidence{
		PreflightStopped: true,
		StopReason:       prefix + "：" + check.Name + " — " + check.Reason,
	}
}

func observedEnqueueAction(policy model.SchedulerPolicy) bool {
	if !policy.Determinate || !policy.ActiveDeterminate {
		return true
	}

	for _, action := range policy.Actions {
		if action == "enqueue" {
			return true
		}
	}

	return false
}

func (a *Agent) completeReport(
	ctx context.Context,
	report *Report,
	scheduler cluster.Scheduler,
	evidence presentationEvidence,
) {
	if scheduler.Name != "" || report.Request.Branch != "" {
		a.applySourceFallback(ctx, report, scheduler, evidence)

		return
	}

	finishReport(report, evidence)
}

func finishReport(report *Report, evidence presentationEvidence) {
	finalizeReport(report)
	buildPresentation(report, evidence)
	synchronizePresentation(report)
	finalizeReport(report)
	report.Diagnosis = diagnoseReport(*report)
	log.Printf(
		"analysis conclusions namespace=%s pod=%s passed=%t enqueue=%s jobValid=%s allocate=%s rootCause=%q",
		report.Request.Namespace,
		report.Request.Pod,
		report.Passed,
		report.Enqueue.State.Outcome,
		report.JobValid.State.Outcome,
		report.Allocate.State.Outcome,
		report.Diagnosis.RootCause,
	)
}

func schedulerEvidenceIdentityCheck(
	policy model.SchedulerPolicy,
	evidence cluster.Scheduler,
) model.Check {
	sources := []string{"clusterManager.CaptureCacheDump"}

	if policy.SchedulerUID == "" || evidence.UID == "" {
		return model.Unknown(
			"scheduler.evidence.identity",
			"allocate",
			"Policy and cache dump use the same scheduler Pod",
			fmt.Sprintf(
				"policy scheduler=%s/%s uid=%q; cache scheduler=%s/%s uid=%q",
				policy.SchedulerNamespace,
				policy.SchedulerPod,
				policy.SchedulerUID,
				evidence.Namespace,
				evidence.Name,
				evidence.UID,
			),
			nil,
			sources,
		)
	}

	same := policy.SchedulerUID == evidence.UID &&
		policy.SchedulerNamespace == evidence.Namespace &&
		policy.SchedulerPod == evidence.Name
	if !same {
		return model.Unknown(
			"scheduler.evidence.identity",
			"allocate",
			"Policy and cache dump use the same scheduler Pod",
			fmt.Sprintf(
				"scheduler leadership changed during analysis: policy=%s/%s uid=%s cache=%s/%s uid=%s",
				policy.SchedulerNamespace,
				policy.SchedulerPod,
				policy.SchedulerUID,
				evidence.Namespace,
				evidence.Name,
				evidence.UID,
			),
			nil,
			sources,
		)
	}

	return model.Known(
		"scheduler.evidence.identity",
		"allocate",
		"Policy and cache dump use the same scheduler Pod",
		true,
		fmt.Sprintf(
			"scheduler=%s/%s uid=%s",
			evidence.Namespace,
			evidence.Name,
			evidence.UID,
		),
		sources,
	)
}

func targetPodLookupCheck(request Request, err error) model.Check {
	name := request.Namespace + "/" + request.Pod

	if apierrors.IsNotFound(err) {
		return model.Known(
			"task.exists",
			"preflight",
			"Target Pod exists",
			false,
			name+": "+err.Error(),
			nil,
		)
	}

	return model.Unknown(
		"task.load",
		"preflight",
		"Target Pod loaded from informer cache",
		name+": "+err.Error(),
		nil,
		nil,
	)
}

func (a *Agent) collectPodGroup(
	ctx context.Context,
	namespace string,
	podGroupName string,
) (cluster.PodGroup, error, []corev1.Pod, error) {
	if podGroupName == "" {
		err := fmt.Errorf("PodGroup association is unavailable")

		return cluster.PodGroup{}, err, nil, err
	}

	group, groupErr := a.clusterManager.GetPodGroup(ctx, namespace, podGroupName)
	tasks, tasksErr := a.clusterManager.ListPodsForPodGroup(ctx, namespace, podGroupName)

	return group, groupErr, tasks, tasksErr
}

func (a *Agent) collectQueue(
	ctx context.Context,
	group cluster.PodGroup,
	groupErr error,
	scheduler cluster.Scheduler,
	schedulerErr error,
) (string, error, cluster.Queue, error) {
	if groupErr != nil {
		err := fmt.Errorf("resolve effective Queue: PodGroup is unavailable: %w", groupErr)

		return "", err, cluster.Queue{}, err
	}

	queueName, queueNameErr := enqueue.ResolveQueueName(group, scheduler, schedulerErr)
	if queueNameErr != nil {
		return queueName, queueNameErr, cluster.Queue{}, queueNameErr
	}

	queue, queueErr := a.clusterManager.GetQueue(ctx, queueName)

	return queueName, nil, queue, queueErr
}

func (a *Agent) collectEnqueueEvents(
	ctx context.Context,
	request Request,
	podGroupName string,
	group cluster.PodGroup,
	groupErr error,
) ([]corev1.Event, error, []corev1.Event, error) {
	podEvents, podEventsErr := a.clusterManager.ListPodEvents(ctx, request.Namespace, request.Pod)

	if groupErr != nil || podGroupName == "" {
		if groupErr == nil {
			groupErr = fmt.Errorf("PodGroup association is unavailable")
		}

		err := fmt.Errorf("collect PodGroup events: PodGroup is unavailable: %w", groupErr)

		return podEvents, podEventsErr, nil, err
	}

	groupEvents, groupEventsErr := a.clusterManager.ListPodGroupEvents(
		ctx,
		request.Namespace,
		podGroupName,
		group.UID,
	)

	return podEvents, podEventsErr, groupEvents, groupEventsErr
}
