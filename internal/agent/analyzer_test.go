package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestAnalyzeRequiresConcreteClusterManager(t *testing.T) {
	agent := New(nil, nil, LLMConfig{})

	_, err := agent.Analyze(context.Background(), Request{
		Namespace: "default",
		Pod:       "pod-a",
	})
	if err == nil || !strings.Contains(err.Error(), "cluster manager") {
		t.Fatalf("err=%v", err)
	}
}

func TestSchedulerEvidenceIdentityCheckAcceptsSamePodUID(t *testing.T) {
	check := schedulerEvidenceIdentityCheck(
		model.SchedulerPolicy{
			SchedulerNamespace: "volcano-system",
			SchedulerPod:       "scheduler-a",
			SchedulerUID:       "uid-a",
		},
		cluster.Scheduler{
			Namespace: "volcano-system",
			Name:      "scheduler-a",
			UID:       "uid-a",
		},
	)

	if !check.Determinate || !check.Passed {
		t.Fatalf("check=%+v", check)
	}
}

func TestSchedulerEvidenceIdentityCheckFlagsLeaderChangeAsUnknown(t *testing.T) {
	check := schedulerEvidenceIdentityCheck(
		model.SchedulerPolicy{
			SchedulerNamespace: "volcano-system",
			SchedulerPod:       "scheduler-a",
			SchedulerUID:       "uid-a",
		},
		cluster.Scheduler{
			Namespace: "volcano-system",
			Name:      "scheduler-b",
			UID:       "uid-b",
		},
	)

	if check.Determinate || check.Passed || !strings.Contains(check.Reason, "leadership changed") {
		t.Fatalf("check=%+v", check)
	}
}

func TestTargetPodLookupCheckClassifiesNotFoundAsKnownFailure(t *testing.T) {
	check := targetPodLookupCheck(
		Request{Namespace: "default", Pod: "pod-a"},
		apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "pod-a"),
	)

	if !check.Determinate || check.Passed {
		t.Fatalf("check=%+v", check)
	}
}

func TestTargetPodLookupCheckClassifiesReadFailureAsUnknown(t *testing.T) {
	check := targetPodLookupCheck(
		Request{Namespace: "default", Pod: "pod-a"},
		errors.New("informer cache unavailable"),
	)

	if check.Determinate || check.Passed {
		t.Fatalf("check=%+v", check)
	}
}

func TestStoppedAtPreflightPreservesKnownAndUnknownReasons(t *testing.T) {
	tests := []struct {
		name       string
		check      model.Check
		wantPrefix string
	}{
		{
			name: "known failure",
			check: model.Known(
				"task.pending",
				"preflight",
				"Pod is Pending",
				false,
				"phase=Running",
				nil,
			),
			wantPrefix: "preflight 明确失败",
		},
		{
			name: "lookup unavailable",
			check: model.Unknown(
				"task.load",
				"preflight",
				"Target Pod loaded from informer cache",
				"cache unavailable",
				nil,
				nil,
			),
			wantPrefix: "preflight 无法完成",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence := stoppedAtPreflight(test.check)

			if !evidence.PreflightStopped {
				t.Fatalf("evidence=%+v", evidence)
			}

			if !strings.HasPrefix(evidence.StopReason, test.wantPrefix) ||
				!strings.Contains(evidence.StopReason, test.check.Name) ||
				!strings.Contains(evidence.StopReason, test.check.Reason) {
				t.Fatalf("stop reason=%q", evidence.StopReason)
			}
		})
	}
}

func TestObservedEnqueueActionOnlyExcludesProvenActivePolicyWithoutEnqueue(t *testing.T) {
	if !observedEnqueueAction(model.SchedulerPolicy{
		Determinate: false,
		Actions:     []string{"enqueue"},
	}) {
		t.Fatal("an unknown action list must keep an explicit rejection reachable")
	}

	if !observedEnqueueAction(model.SchedulerPolicy{
		Determinate:       true,
		ActiveDeterminate: false,
		Actions:           []string{"allocate", "backfill"},
	}) {
		t.Fatal("an observed but unconfirmed policy must keep enqueue possibly reachable")
	}

	if observedEnqueueAction(model.SchedulerPolicy{
		Determinate:       true,
		ActiveDeterminate: true,
		Actions:           []string{"allocate", "backfill"},
	}) {
		t.Fatal("a proven active allocate/backfill policy must exclude the enqueue gate")
	}

}

func TestRegisteredAnalysisRulesKeepSchedulingOrder(t *testing.T) {
	rules := analysisRuleSnapshot()
	names := make([]string, 0, len(rules))

	for _, rule := range rules {
		names = append(names, rule.Name())
	}

	want := "validate,enqueue,jobValid,allocate"
	if strings.Join(names, ",") != want {
		t.Fatalf("rules=%v want=%s", names, want)
	}
}
