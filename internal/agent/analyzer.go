package agent

import (
	"context"
	"fmt"

	"github.com/volcano-sh/volens/internal/agent/enqueue"
	"github.com/volcano-sh/volens/internal/agent/filter"
	"github.com/volcano-sh/volens/internal/agent/validate"
	"github.com/volcano-sh/volens/internal/cluster"
	"github.com/volcano-sh/volens/internal/source"
	corev1 "k8s.io/api/core/v1"
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

func (a *Agent) Analyze(ctx context.Context, request Request) (Report, error) {
	if request.Namespace == "" || request.Pod == "" {
		return Report{}, fmt.Errorf("namespace and pod are required")
	}

	if a.clusterManager == nil {
		return Report{}, fmt.Errorf("cluster manager is not configured")
	}

	pod, err := a.clusterManager.GetPod(ctx, request.Namespace, request.Pod)
	if err != nil {
		return Report{}, err
	}

	scheduler, schedulerErr := a.clusterManager.GetVolcanoScheduler(ctx)

	report := Report{
		Request:   request,
		Scheduler: scheduler,
	}
	report.Checks = append(report.Checks, validate.Evaluate(pod, scheduler, schedulerErr)...)
	finalizeReport(&report)

	if firstKnownFailure(report) != "" {
		return report, nil
	}

	podGroup := validate.PodGroupName(pod)
	group, groupErr := a.clusterManager.GetPodGroup(ctx, request.Namespace, podGroup)
	tasks, tasksErr := a.clusterManager.ListPodsForPodGroup(ctx, request.Namespace, podGroup)
	report.Checks = append(
		report.Checks,
		validate.EvaluatePodGroup(podGroup, group, groupErr, tasks, tasksErr)...,
	)
	finalizeReport(&report)

	if firstKnownFailure(report) != "" {
		return report, nil
	}

	queueName, queueNameErr := enqueue.ResolveQueueName(group, scheduler, schedulerErr)

	var queue cluster.Queue
	var queueErr error

	if groupErr != nil {
		queueErr = groupErr
	} else if queueNameErr == nil {
		queue, queueErr = a.clusterManager.GetQueue(ctx, queueName)
	}

	podEvents, podEventsErr := a.clusterManager.ListPodEvents(ctx, request.Namespace, request.Pod)
	var groupEventsErr error
	var groupEvents []corev1.Event

	if groupErr != nil {
		groupEventsErr = groupErr
	} else {
		groupEvents, groupEventsErr = a.clusterManager.ListPodGroupEvents(
			ctx,
			request.Namespace,
			podGroup,
			group.UID,
		)
	}

	report.Checks = append(report.Checks, enqueue.Evaluate(enqueue.Input{
		SchedulerName:  pod.Spec.SchedulerName,
		PodGroup:       group,
		PodGroupErr:    groupErr,
		QueueName:      queueName,
		QueueNameErr:   queueNameErr,
		Queue:          queue,
		QueueErr:       queueErr,
		PodEvents:      podEvents,
		PodEventsErr:   podEventsErr,
		GroupEvents:    groupEvents,
		GroupEventsErr: groupEventsErr,
	})...)
	finalizeReport(&report)

	if firstKnownFailure(report) != "" {
		return report, nil
	}

	nodes, err := a.clusterManager.ListNodes(ctx)
	if err != nil {
		return Report{}, err
	}

	dump, cacheErr := a.clusterManager.CaptureCacheDump(ctx)
	if dump.Scheduler.Name != "" {
		scheduler = dump.Scheduler
		report.Scheduler = scheduler
	}

	filterResult := filter.Evaluate(filter.Input{
		Pod:        pod,
		Nodes:      nodes,
		Dump:       dump,
		CaptureErr: cacheErr,
	})
	report.Checks = append(report.Checks, filterResult.CacheCheck)
	report.Nodes = filterResult.Nodes
	report.Checks = append(
		report.Checks,
		filterResult.AllocationCheck,
		filterResult.PluginCheck,
	)

	finalizeReport(&report)

	if firstKnownFailure(report) == "" && (scheduler.Name != "" || request.Branch != "") {
		a.applySourceFallback(ctx, &report, scheduler)
	}

	return report, nil
}
