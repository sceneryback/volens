package enqueue

import (
	"fmt"
	"strings"
	"time"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

var enqueueSources = []string{
	"pkg/scheduler/actions/enqueue/enqueue.go",
	"pkg/scheduler/framework/session_plugins.go:JobEnqueueable",
}

var queueSources = []string{
	"cmd/scheduler/app/options/options.go",
	"pkg/scheduler/cache/event_handlers.go:setPodGroup",
	"pkg/scheduler/actions/enqueue/enqueue.go",
}

type Input struct {
	SchedulerName      string
	Actions            []string
	ActionsDeterminate bool
	ActiveDeterminate  bool
	PodGroup           cluster.PodGroup
	PodGroupErr        error
	QueueName          string
	QueueNameErr       error
	Queue              cluster.Queue
	QueueErr           error
	PodEvents          []corev1.Event
	PodEventsErr       error
	GroupEvents        []corev1.Event
	GroupEventsErr     error
}

func ResolveQueueName(
	podGroup cluster.PodGroup,
	scheduler cluster.Scheduler,
	schedulerErr error,
) (string, error) {
	if podGroup.Queue != "" {
		return podGroup.Queue, nil
	}

	if schedulerErr != nil {
		return "", fmt.Errorf("resolve scheduler default queue: %w", schedulerErr)
	}

	if !scheduler.ConfigurationDeterminate {
		return "", fmt.Errorf(
			"resolve scheduler default queue: %s",
			scheduler.ConfigurationReason,
		)
	}

	if scheduler.DefaultQueue == "" {
		return "", fmt.Errorf("resolve scheduler default queue: parsed value is empty")
	}

	return scheduler.DefaultQueue, nil
}

func Evaluate(input Input) []model.Check {
	events := append([]corev1.Event(nil), input.PodEvents...)
	events = append(events, input.GroupEvents...)
	checks := []model.Check{evaluateQueue(input)}
	standaloneEnqueue := containsAction(input.Actions, "enqueue")

	if input.ActionsDeterminate && !standaloneEnqueue && input.ActiveDeterminate {
		reason := "the observed scheduler actions do not contain enqueue; PodGroup phase and old Events are not a currently reachable enqueue gate"
		checks = append(checks,
			model.Skipped(
				"job.enqueue.evidence",
				"enqueue",
				"Enqueue evidence",
				reason,
				enqueueSources,
			),
			model.Skipped(
				"plugins.job-enqueueable",
				"enqueue",
				"Active JobEnqueueable plugin hooks",
				reason,
				enqueueSources,
			),
		)

		return checks
	}

	if input.ActionsDeterminate && !standaloneEnqueue {
		reason := "the observed scheduler actions do not contain enqueue, but that ConfigMap version is not proven active in the scheduler's in-memory Session; PodGroup phase and Events cannot prove the current enqueue gate"
		evidence := map[string]any{
			"observedActions":   append([]string(nil), input.Actions...),
			"activeDeterminate": input.ActiveDeterminate,
		}
		checks = append(checks,
			model.Unknown(
				"job.enqueue.evidence",
				"enqueue",
				"Enqueue evidence",
				reason,
				evidence,
				enqueueSources,
			),
			model.Unknown(
				"plugins.job-enqueueable",
				"enqueue",
				"Active JobEnqueueable plugin hooks",
				reason,
				evidence,
				enqueueSources,
			),
		)

		return checks
	}

	if input.ActionsDeterminate && standaloneEnqueue &&
		input.PodGroupErr == nil && !podGroupInQueue(input.PodGroup) {
		checks = append(checks,
			model.Known(
				"job.enqueue.evidence",
				"enqueue",
				"Enqueue evidence",
				false,
				"PodGroup phase="+input.PodGroup.Phase+"; Volcano allocate only schedules jobs after they enter Inqueue/Running",
				enqueueSources,
			),
			evaluatePluginHooks(input, enqueueRejected),
		)

		return checks
	}

	if podGroupInQueue(input.PodGroup) && input.PodGroupErr == nil {
		checks = append(checks,
			model.Known(
				"job.enqueue.evidence",
				"enqueue",
				"Enqueue evidence",
				true,
				"PodGroup phase="+input.PodGroup.Phase,
				enqueueSources,
			),
			evaluatePluginHooks(input, enqueueSucceeded),
		)

		return checks
	}

	event, outcome := newestEnqueueEvidence(events, input.SchedulerName)

	if outcome == enqueueRejected || outcome == enqueueSucceeded {
		reason := eventReason(event) +
			"; this Event is historical evidence and is not tied to the current scheduler Session, PodGroup transition, or queue resource snapshot"
		checks = append(checks,
			model.Unknown(
				"job.enqueue.evidence",
				"enqueue",
				"Enqueue evidence",
				reason,
				event,
				enqueueSources,
			),
			evaluatePluginHooks(input, outcome),
		)

		return checks
	}

	reason := "no Pod or PodGroup event proves enqueue success or rejection"

	if input.PodEventsErr != nil || input.GroupEventsErr != nil {
		reason = eventErrors(input.PodEventsErr, input.GroupEventsErr)
	}

	checks = append(checks,
		model.Unknown(
			"job.enqueue.evidence",
			"enqueue",
			"Enqueue evidence",
			reason,
			nil,
			enqueueSources,
		),
		evaluatePluginHooks(input, outcome),
	)

	return checks
}

func containsAction(actions []string, wanted string) bool {
	for _, action := range actions {
		if strings.TrimSpace(action) == wanted {
			return true
		}
	}

	return false
}

func evaluateQueue(input Input) model.Check {
	if input.QueueNameErr != nil {
		return model.Unknown(
			"job.queue.exists",
			"enqueue",
			"Effective Queue exists",
			input.QueueNameErr.Error(),
			nil,
			queueSources,
		)
	}

	if input.QueueErr != nil {
		if apierrors.IsNotFound(input.QueueErr) {
			return model.Known(
				"job.queue.exists",
				"enqueue",
				"Queue exists",
				false,
				input.QueueErr.Error(),
				enqueueSources,
			)
		}

		return model.Unknown(
			"job.queue.exists",
			"enqueue",
			"Queue exists",
			input.QueueErr.Error(),
			nil,
			enqueueSources,
		)
	}

	check := model.Known(
		"job.queue.exists",
		"enqueue",
		"Queue exists",
		input.Queue.Name == input.QueueName,
		fmt.Sprintf("queue=%s state=%s", input.Queue.Name, input.Queue.State),
		queueSources,
	)
	check.Evidence = input.Queue

	return check
}

func evaluatePluginHooks(input Input, outcome enqueueOutcome) model.Check {
	reason := "active actions, plugin tiers, vote short-circuiting, arguments, and Session state are not reproducible from the available evidence"

	if input.PodGroupErr == nil && input.PodGroup.MinResources == nil {
		reason = "PodGroup minResources is nil: the standard enqueue action would bypass JobEnqueueable, but the active action configuration is not available"
	} else if input.PodGroupErr == nil &&
		(outcome == enqueueSucceeded || podGroupInQueue(input.PodGroup)) {
		reason = "PodGroup entered the queue, but allocate can do that when the enqueue action is disabled, so JobEnqueueable execution is not proven"
	}

	return model.Unknown(
		"plugins.job-enqueueable",
		"enqueue",
		"Active JobEnqueueable plugin hooks",
		reason,
		nil,
		enqueueSources,
	)
}

func podGroupInQueue(podGroup cluster.PodGroup) bool {
	return strings.EqualFold(podGroup.Phase, "Inqueue") ||
		strings.EqualFold(podGroup.Phase, "Running")
}

type enqueueOutcome int

const (
	enqueueUnknown enqueueOutcome = iota
	enqueueRejected
	enqueueSucceeded
)

func newestEnqueueEvidence(events []corev1.Event, schedulerName string) (corev1.Event, enqueueOutcome) {
	var selected corev1.Event
	selectedOutcome := enqueueUnknown
	selectedTime := time.Time{}

	for _, event := range events {
		if !schedulerEvent(event, schedulerName) {
			continue
		}

		outcome := classifyEnqueueEvent(event)
		if outcome == enqueueUnknown {
			continue
		}

		observed := eventObservedTime(event)
		if selectedOutcome == enqueueUnknown || observed.After(selectedTime) || observed.Equal(selectedTime) {
			selected = event
			selectedOutcome = outcome
			selectedTime = observed
		}
	}

	return selected, selectedOutcome
}

func classifyEnqueueEvent(event corev1.Event) enqueueOutcome {
	reason := strings.ToLower(strings.TrimSpace(event.Reason))
	message := strings.ToLower(strings.TrimSpace(event.Message))

	if reason == "enqueued" || strings.Contains(message, "job enqueued") || strings.Contains(message, "podgroup enqueued") {
		return enqueueSucceeded
	}

	if strings.EqualFold(event.InvolvedObject.Kind, "PodGroup") &&
		strings.Contains(message, "queue resource quota insufficient") {
		return enqueueRejected
	}

	if strings.Contains(reason, "enqueue") &&
		(strings.Contains(reason, "fail") || strings.Contains(reason, "reject")) {
		return enqueueRejected
	}

	return enqueueUnknown
}

func schedulerEvent(event corev1.Event, schedulerName string) bool {
	expected := strings.ToLower(strings.TrimSpace(schedulerName))

	for _, component := range []string{event.Source.Component, event.ReportingController} {
		component = strings.ToLower(strings.TrimSpace(component))

		if component == "" {
			continue
		}

		if component == expected || strings.Contains(component, "volcano") || strings.Contains(component, "vc-scheduler") {
			return true
		}
	}

	return false
}

func eventObservedTime(event corev1.Event) time.Time {
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}

	if event.Series != nil && !event.Series.LastObservedTime.IsZero() {
		return event.Series.LastObservedTime.Time
	}

	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}

	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}

	return event.CreationTimestamp.Time
}

func eventReason(event corev1.Event) string {
	return strings.TrimSpace(event.Reason + " " + event.Message)
}

func eventErrors(podErr, groupErr error) string {
	parts := make([]string, 0, 2)

	if podErr != nil {
		parts = append(parts, "Pod events: "+podErr.Error())
	}

	if groupErr != nil {
		parts = append(parts, "PodGroup events: "+groupErr.Error())
	}

	return fmt.Sprintf("event evidence is incomplete (%s)", strings.Join(parts, "; "))
}
