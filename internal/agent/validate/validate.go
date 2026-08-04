package validate

import (
	"errors"
	"fmt"
	"sort"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const taskSpecAnnotation = "volcano.sh/task-spec"

var jobValidSources = []string{
	"pkg/scheduler/actions/allocate/allocate.go:JobValid",
	"pkg/scheduler/actions/allocate/v2/allocate.go:JobValid",
	"pkg/scheduler/framework/session_plugins.go:JobValid",
	"pkg/scheduler/framework/session.go:OpenSession",
}

var schedulerConfigurationSources = []string{
	"cmd/scheduler/app/options/options.go",
	"pkg/scheduler/cache/event_handlers.go:getOrCreateJob",
	"pkg/scheduler/api/job_info.go:getJobID",
}

func Evaluate(
	pod *corev1.Pod,
	scheduler cluster.Scheduler,
	schedulerErr error,
) []model.Check {
	podGroup := podGroupName(pod)
	podGroupReason := "scheduling.k8s.io/group-name=" + podGroup

	if podGroup == "" && pod.Labels["volcano.sh/job-name"] != "" {
		podGroupReason = "scheduling.k8s.io/group-name is missing; volcano.sh/job-name is a VolcanoJob label, not the scheduler PodGroup association"
	}

	checks := []model.Check{
		model.Known(
			"task.pending",
			"validate",
			"Pod is Pending",
			pod.Status.Phase == corev1.PodPending,
			string(pod.Status.Phase),
			nil,
		),
		evaluateSchedulerName(pod.Spec.SchedulerName, scheduler, schedulerErr),
		model.Known(
			"task.unbound",
			"validate",
			"Pod has not been assigned to a node",
			pod.Spec.NodeName == "",
			"spec.nodeName="+pod.Spec.NodeName+"; assigned Pending Pods require kubelet, runtime, image, or volume debugging instead of scheduler analysis",
			nil,
		),
		model.Known(
			"task.active",
			"validate",
			"Pod is not terminating",
			pod.DeletionTimestamp == nil,
			"deletionTimestamp must be empty",
			nil,
		),
		model.Known(
			"task.scheduling-gates",
			"validate",
			"Pod has no scheduling gates",
			len(pod.Spec.SchedulingGates) == 0,
			fmt.Sprintf("schedulingGates=%v", pod.Spec.SchedulingGates),
			nil,
		),
		model.Known(
			"task.containers",
			"validate",
			"Has containers",
			len(pod.Spec.Containers) > 0,
			"Pod must contain at least one container",
			nil,
		),
		model.Known(
			"job.podgroup",
			"validate",
			"PodGroup association",
			podGroup != "",
			podGroupReason,
			schedulerConfigurationSources,
		),
	}

	return checks
}

func evaluateSchedulerName(
	podSchedulerName string,
	scheduler cluster.Scheduler,
	schedulerErr error,
) model.Check {
	if errors.Is(schedulerErr, cluster.ErrVolcanoSchedulerNotReady) {
		return model.Known(
			"task.scheduler",
			"validate",
			"Ready Volcano scheduler instance exists",
			false,
			schedulerErr.Error(),
			schedulerConfigurationSources,
		)
	}

	if schedulerErr != nil {
		return model.Unknown(
			"task.scheduler",
			"validate",
			"Scheduler name is handled by this Volcano instance",
			schedulerErr.Error(),
			nil,
			schedulerConfigurationSources,
		)
	}

	if !scheduler.ConfigurationDeterminate {
		return model.Unknown(
			"task.scheduler",
			"validate",
			"Scheduler name is handled by this Volcano instance",
			scheduler.ConfigurationReason,
			scheduler.SchedulerNames,
			schedulerConfigurationSources,
		)
	}

	passed := false

	for _, name := range scheduler.SchedulerNames {
		if podSchedulerName == name {
			passed = true

			break
		}
	}

	check := model.Known(
		"task.scheduler",
		"validate",
		"Scheduler name is handled by this Volcano instance",
		passed,
		fmt.Sprintf("pod schedulerName=%q configured names=%v", podSchedulerName, scheduler.SchedulerNames),
		schedulerConfigurationSources,
	)
	check.Evidence = scheduler.SchedulerNames

	return check
}

func EvaluatePodGroup(
	podGroupName string,
	podGroup cluster.PodGroup,
	podGroupErr error,
	tasks []corev1.Pod,
	tasksErr error,
) []model.Check {
	checks := make([]model.Check, 0, 4)

	if podGroupErr != nil {
		if apierrors.IsNotFound(podGroupErr) {
			checks = append(checks, model.Known(
				"job.podgroup.exists",
				"validate",
				"PodGroup exists",
				false,
				podGroupErr.Error(),
				jobValidSources,
			))
		} else {
			checks = append(checks, model.Unknown(
				"job.podgroup.exists",
				"validate",
				"PodGroup exists",
				podGroupErr.Error(),
				nil,
				jobValidSources,
			))
		}

		return append(checks, unknownPluginCheck())
	}

	exists := podGroup.Name == podGroupName && podGroup.Namespace != ""
	podGroupCheck := model.Known(
		"job.podgroup.exists",
		"validate",
		"PodGroup exists",
		exists,
		fmt.Sprintf("PodGroup=%s/%s phase=%s", podGroup.Namespace, podGroup.Name, podGroup.Phase),
		jobValidSources,
	)
	podGroupCheck.Evidence = podGroup
	checks = append(checks, podGroupCheck)

	if !exists {
		return append(checks, unknownPluginCheck())
	}

	checks = append(checks, evaluateGangTasks(podGroup, tasks, tasksErr)...)
	checks = append(checks, unknownPluginCheck())

	return checks
}

func evaluateGangTasks(
	podGroup cluster.PodGroup,
	tasks []corev1.Pod,
	tasksErr error,
) []model.Check {
	if tasksErr != nil {
		return []model.Check{
			model.Unknown(
				"plugins.gang.tasks",
				"validate",
				"Gang minimum task counts",
				tasksErr.Error(),
				nil,
				[]string{"pkg/scheduler/plugins/gang/gang.go:validJobFn"},
			),
		}
	}

	validTasks := int32(0)
	taskCounts := map[string]int32{}

	for i := range tasks {
		task := &tasks[i]

		switch task.Status.Phase {
		case corev1.PodPending, corev1.PodRunning:
			if task.DeletionTimestamp != nil {
				continue
			}
		case corev1.PodSucceeded:
		default:
			continue
		}

		validTasks++
		taskCounts[task.Annotations[taskSpecAnnotation]]++
	}

	minimumPassed := validTasks >= podGroup.MinMember
	minimumCheck := model.Unknown(
		"plugins.gang.min-member",
		"validate",
		"Active gang minMember rule",
		fmt.Sprintf(
			"calculated valid tasks=%d minMember=%d wouldPass=%t; active gang plugin configuration is unavailable",
			validTasks,
			podGroup.MinMember,
			minimumPassed,
		),
		nil,
		[]string{"pkg/scheduler/plugins/gang/gang.go:validJobFn"},
	)
	minimumCheck.Evidence = map[string]any{
		"validTasks": validTasks,
		"minMember":  podGroup.MinMember,
		"wouldPass":  minimumPassed,
		"taskCounts": taskCounts,
	}

	checks := []model.Check{minimumCheck}
	minimumTaskTotal := int32(0)

	for _, minimum := range podGroup.MinTaskMember {
		minimumTaskTotal += minimum
	}

	if podGroup.MinMember < minimumTaskTotal {
		return checks
	}

	taskNames := make([]string, 0, len(podGroup.MinTaskMember))

	for taskName := range podGroup.MinTaskMember {
		taskNames = append(taskNames, taskName)
	}

	sort.Strings(taskNames)

	for _, taskName := range taskNames {
		minimum := podGroup.MinTaskMember[taskName]

		wouldPass := taskCounts[taskName] >= minimum

		checks = append(checks, model.Unknown(
			"plugins.gang.min-task-member."+taskName,
			"validate",
			"Active gang minTaskMember "+taskName,
			fmt.Sprintf(
				"calculated valid tasks=%d minimum=%d wouldPass=%t; active gang plugin configuration is unavailable",
				taskCounts[taskName],
				minimum,
				wouldPass,
			),
			map[string]any{
				"validTasks": taskCounts[taskName],
				"minimum":    minimum,
				"wouldPass":  wouldPass,
			},
			[]string{"pkg/scheduler/plugins/gang/gang.go:validJobFn"},
		))
	}

	return checks
}

func unknownPluginCheck() model.Check {
	return model.Unknown(
		"plugins.job-valid",
		"validate",
		"Remaining active JobValid plugin hooks",
		"branch-specific hooks depend on the scheduler Session and plugin-private state; the common gang rules were evaluated separately",
		nil,
		jobValidSources,
	)
}

func PodGroupName(pod *corev1.Pod) string {
	return podGroupName(pod)
}

func podGroupName(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}

	return pod.Annotations["scheduling.k8s.io/group-name"]
}
