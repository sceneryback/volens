package agent

import (
	"strings"
	"testing"
)

func TestFinalizeReportIncludesEveryKnownFailure(t *testing.T) {
	report := Report{
		Checks: []Check{
			{
				Stage:       "validate",
				Name:        "first failure",
				Determinate: true,
				Reason:      "reason one",
			},
			{
				Stage:       "enqueue",
				Name:        "unknown hook",
				Determinate: false,
				Reason:      "needs source",
			},
			{
				Stage:       "allocate",
				Name:        "second failure",
				Determinate: true,
				Reason:      "reason two",
			},
		},
	}

	finalizeReport(&report)

	for _, expected := range []string{
		"Found 2 deterministic failure(s)",
		"first failure: reason one",
		"second failure: reason two",
	} {
		if !strings.Contains(report.Conclusion, expected) {
			t.Fatalf("conclusion %q does not contain %q", report.Conclusion, expected)
		}
	}
}

func TestBuildActionReportsPreservesObservedOrderAndGroupsNodes(t *testing.T) {
	report := Report{
		Checks: []Check{
			{Stage: "preflight", Name: "pod"},
			{Stage: "enqueue", Name: "queue"},
			{Stage: "enqueue", Name: "hooks"},
		},
		Nodes: []NodeResult{
			{
				Name: "node-a",
				Checks: []Check{
					{
						Stage:       "allocate",
						Name:        "resources",
						Passed:      true,
						Determinate: true,
					},
				},
			},
		},
	}

	actions := buildActionReports(report)

	if len(actions) != 3 {
		t.Fatalf("actions=%+v", actions)
	}

	if actions[0].Name != "preflight" || actions[1].Name != "enqueue" || actions[2].Name != "allocate" {
		t.Fatalf("action order=%+v", actions)
	}

	if len(actions[1].Checks) != 2 {
		t.Fatalf("enqueue checks=%+v", actions[1].Checks)
	}

	if len(actions[2].Nodes) != 1 || actions[2].Nodes[0].Name != "node-a" {
		t.Fatalf("allocate nodes=%+v", actions[2].Nodes)
	}
}

func TestBuildActionReportsUsesConfiguredRuntimeOrderBeforeSupplementalChecks(t *testing.T) {
	report := Report{
		Policy: SchedulerPolicy{
			Actions: []string{"allocate", "backfill"},
		},
		Checks: []Check{
			{Stage: "preflight", Name: "pod"},
			{Stage: "enqueue", Name: "enqueue evidence"},
			{Stage: "allocate", Name: "filters"},
		},
		PluginHooks: []PluginHookReport{
			{Action: "allocate", Plugin: "predicates", Hook: "AddPredicateFn"},
		},
	}

	actions := buildActionReports(report)

	if len(actions) != 4 {
		t.Fatalf("actions=%+v", actions)
	}

	want := []string{"preflight", "allocate", "backfill", "enqueue"}

	for index, name := range want {
		if actions[index].Name != name {
			t.Fatalf("actions[%d]=%+v want=%s", index, actions[index], name)
		}
	}

	if !actions[1].Configured || actions[1].Order != 0 || len(actions[1].Plugins) != 1 {
		t.Fatalf("allocate=%+v", actions[1])
	}

	if len(actions[2].Checks) != 1 || actions[2].Checks[0].ID != "action.backfill.coverage" {
		t.Fatalf("backfill=%+v", actions[2])
	}

	if actions[3].Configured {
		t.Fatalf("supplemental enqueue=%+v", actions[3])
	}
}

func TestBuildActionReportsPreservesRepeatedConfiguredActionOccurrences(t *testing.T) {
	report := Report{
		Policy: SchedulerPolicy{Actions: []string{"enqueue", "enqueue"}},
		Checks: []Check{
			{Stage: "enqueue", Name: "queue"},
		},
		PluginHooks: []PluginHookReport{
			{Action: "enqueue", Plugin: "gang", Hook: "AddJobEnqueueableFn"},
		},
	}

	actions := buildActionReports(report)

	if len(actions) != 2 {
		t.Fatalf("actions=%+v", actions)
	}

	for index, action := range actions {
		if action.Name != "enqueue" || action.Order != index ||
			len(action.Checks) != 1 || len(action.Plugins) != 1 {
			t.Fatalf("actions[%d]=%+v", index, action)
		}
	}
}
