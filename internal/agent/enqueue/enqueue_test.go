package enqueue

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEvaluateFindsPodGroupRejection(t *testing.T) {
	input := baseInput()
	input.GroupEvents = []corev1.Event{
		{
			Type:    corev1.EventTypeNormal,
			Reason:  "Unschedulable",
			Message: "queue quota is insufficient",
			InvolvedObject: corev1.ObjectReference{
				Kind: "PodGroup",
			},
			Source: corev1.EventSource{
				Component: "volcano",
			},
		},
	}
	checks := Evaluate(input)

	if checks[1].Passed || !checks[1].Determinate {
		t.Fatalf("evidence check=%+v", checks[1])
	}

	if !strings.Contains(checks[1].Reason, "quota") {
		t.Fatalf("reason=%q", checks[1].Reason)
	}
}

func TestEvaluateAcceptsExplicitEnqueueSuccess(t *testing.T) {
	input := baseInput()
	input.PodEvents = []corev1.Event{
		{
			Reason:  "Enqueued",
			Message: "job enqueued",
			Source: corev1.EventSource{
				Component: "volcano",
			},
		},
	}
	checks := Evaluate(input)

	if !checks[1].Passed || !checks[1].Determinate {
		t.Fatalf("enqueue check=%+v", checks[1])
	}

	if checks[2].Passed || checks[2].Determinate || !strings.Contains(checks[2].Reason, "bypass") {
		t.Fatalf("plugin check=%+v", checks[2])
	}
}

func TestEvaluateUsesNewestSchedulerEvidence(t *testing.T) {
	oldTime := metav1.NewTime(time.Unix(100, 0))
	newTime := metav1.NewTime(time.Unix(200, 0))
	input := baseInput()
	input.GroupEvents = []corev1.Event{
		{
			Type:    corev1.EventTypeNormal,
			Reason:  "Unschedulable",
			Message: "resource in cluster is overused",
			InvolvedObject: corev1.ObjectReference{
				Kind: "PodGroup",
			},
			Source:        corev1.EventSource{Component: "volcano"},
			LastTimestamp: oldTime,
		},
		{
			Reason:        "Enqueued",
			Message:       "job enqueued",
			Source:        corev1.EventSource{Component: "volcano"},
			LastTimestamp: newTime,
		},
	}
	checks := Evaluate(input)

	if !checks[1].Passed || !strings.Contains(checks[1].Reason, "job enqueued") {
		t.Fatalf("enqueue check=%+v", checks[1])
	}
}

func TestEvaluateIgnoresNonSchedulerEvent(t *testing.T) {
	input := baseInput()
	input.PodEvents = []corev1.Event{
		{
			Reason:  "FailedEnqueue",
			Message: "queue rejected by unrelated controller",
			Source:  corev1.EventSource{Component: "other-controller"},
		},
	}
	checks := Evaluate(input)

	if checks[1].Determinate {
		t.Fatalf("check=%+v", checks[1])
	}
}

func TestEvaluateWithoutEvidenceKeepsOnlyUnknownEvidence(t *testing.T) {
	checks := Evaluate(baseInput())

	if !checks[0].Passed || !checks[0].Determinate {
		t.Fatalf("queue check=%+v", checks[0])
	}

	if checks[1].Passed || checks[1].Determinate {
		t.Fatalf("evidence check=%+v", checks[1])
	}

	if checks[2].Passed || checks[2].Determinate {
		t.Fatalf("plugin action check=%+v", checks[2])
	}
}

func TestEvaluateNonNilMinResourcesKeepsPluginUnknown(t *testing.T) {
	input := baseInput()
	input.PodGroup.MinResources = corev1.ResourceList{}
	checks := Evaluate(input)

	if checks[2].Passed || checks[2].Determinate {
		t.Fatalf("plugin check=%+v", checks[2])
	}
}

func TestEvaluateInqueuePhaseWinsOverLaterUnschedulableEvent(t *testing.T) {
	input := baseInput()
	input.PodGroup.Phase = "Inqueue"
	input.PodGroup.MinResources = corev1.ResourceList{}
	input.GroupEvents = []corev1.Event{
		{
			Type:    corev1.EventTypeWarning,
			Reason:  "Unschedulable",
			Message: "gang allocation currently has no feasible node",
			InvolvedObject: corev1.ObjectReference{
				Kind: "PodGroup",
			},
			Source:        corev1.EventSource{Component: "volcano"},
			LastTimestamp: metav1.NewTime(time.Unix(300, 0)),
		},
	}

	checks := Evaluate(input)

	if !checks[1].Passed || !checks[1].Determinate ||
		!strings.Contains(checks[1].Reason, "Inqueue") {
		t.Fatalf("enqueue check=%+v", checks[1])
	}

	if checks[2].Passed || checks[2].Determinate ||
		!strings.Contains(checks[2].Reason, "not proven") {
		t.Fatalf("plugin check=%+v", checks[2])
	}
}

func TestEvaluateDoesNotTreatWarningUnschedulableAsEnqueueRejection(t *testing.T) {
	input := baseInput()
	input.GroupEvents = []corev1.Event{
		{
			Type:    corev1.EventTypeWarning,
			Reason:  "Unschedulable",
			Message: "predicate failed after enqueue",
			InvolvedObject: corev1.ObjectReference{
				Kind: "PodGroup",
			},
			Source: corev1.EventSource{Component: "volcano"},
		},
	}

	checks := Evaluate(input)

	if checks[1].Determinate {
		t.Fatalf("enqueue check=%+v", checks[1])
	}
}

func TestEvaluateDoesNotTreatPodUnschedulableAsEnqueueRejection(t *testing.T) {
	input := baseInput()
	input.PodEvents = []corev1.Event{
		{
			Reason:  "Unschedulable",
			Message: "node predicate rejected the Pod",
			InvolvedObject: corev1.ObjectReference{
				Kind: "Pod",
			},
			Source: corev1.EventSource{Component: "volcano"},
		},
	}

	checks := Evaluate(input)

	if checks[1].Determinate {
		t.Fatalf("enqueue check=%+v", checks[1])
	}
}

func TestEvaluateEventErrorsAreUnknown(t *testing.T) {
	input := baseInput()
	input.PodEventsErr = errors.New("pod denied")
	input.GroupEventsErr = errors.New("group denied")
	checks := Evaluate(input)

	if checks[1].Determinate || !strings.Contains(checks[1].Reason, "PodGroup events") {
		t.Fatalf("check=%+v", checks[1])
	}
}

func TestResolveQueueNameUsesRuntimeDefault(t *testing.T) {
	podGroup := cluster.PodGroup{}
	scheduler := cluster.Scheduler{
		DefaultQueue:             "batch-default",
		ConfigurationDeterminate: true,
	}

	queue, err := ResolveQueueName(podGroup, scheduler, nil)
	if err != nil || queue != "batch-default" {
		t.Fatalf("queue=%q err=%v", queue, err)
	}

	podGroup.Queue = "explicit"
	queue, err = ResolveQueueName(podGroup, cluster.Scheduler{}, errors.New("scheduler unavailable"))
	if err != nil || queue != "explicit" {
		t.Fatalf("explicit queue=%q err=%v", queue, err)
	}
}

func TestResolveQueueNameKeepsUnparsedDefaultUnknown(t *testing.T) {
	_, err := ResolveQueueName(
		cluster.PodGroup{},
		cluster.Scheduler{ConfigurationReason: "shell wrapper"},
		nil,
	)

	if err == nil || !strings.Contains(err.Error(), "shell wrapper") {
		t.Fatalf("err=%v", err)
	}
}

func baseInput() Input {
	return Input{
		SchedulerName: "volcano",
		PodGroup: cluster.PodGroup{
			Namespace: "default",
			Name:      "job-a",
			Queue:     "default",
			Phase:     "Pending",
		},
		QueueName: "default",
		Queue: cluster.Queue{
			Name:  "default",
			State: "Open",
		},
	}
}
