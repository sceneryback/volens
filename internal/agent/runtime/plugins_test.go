package runtime

import (
	"errors"
	"strings"
	"testing"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/source"
)

func TestInspectPluginHooksUsesConfigOrderSourceRegistrationAndBranchDefaults(t *testing.T) {
	policy := model.SchedulerPolicy{
		Actions: []string{"enqueue", "allocate"},
		Tiers: []model.PluginTier{
			{
				Order: 0,
				Plugins: []model.ConfiguredPlugin{
					{
						Name:  "custom",
						Tier:  0,
						Order: 0,
						ExplicitOptions: map[string]bool{
							"enableJobEnqueued": false,
						},
					},
					{Name: "predicates", Tier: 0, Order: 1},
				},
			},
		},
	}
	sources := []source.SourceFile{
		{
			Path:        "pkg/scheduler/plugins/custom/register.go",
			PluginNames: []string{"custom"},
			Hooks:       []string{"AddJobEnqueueableFn"},
		},
		{
			Path:        "pkg/scheduler/plugins/predicates/predicates.go",
			PluginNames: []string{"predicates"},
			Hooks:       []string{"AddPredicateFn"},
		},
	}

	reports := InspectPluginHooks(
		policy,
		sources,
		map[string]bool{"enablePredicate": true},
		nil,
	)

	custom := findPluginHook(t, reports, "enqueue", "custom", "AddJobEnqueueableFn")
	if !custom.EnabledDeterminate || custom.Enabled || !custom.Determinate || !custom.Passed {
		t.Fatalf("custom=%+v", custom)
	}

	if custom.Phase != 2 {
		t.Fatalf("custom phase=%d", custom.Phase)
	}

	predicates := findPluginHook(t, reports, "allocate", "predicates", "AddPredicateFn")
	if !predicates.EnabledDeterminate || !predicates.Enabled || predicates.Determinate || predicates.Passed {
		t.Fatalf("predicates=%+v", predicates)
	}

	if predicates.Tier != 0 || predicates.Order != 1 {
		t.Fatalf("predicates order=%+v", predicates)
	}

	if predicates.Phase != 7 {
		t.Fatalf("predicates phase=%d", predicates.Phase)
	}
}

func TestInspectPluginHooksDoesNotGuessMissingBranchDefault(t *testing.T) {
	policy := model.SchedulerPolicy{
		Actions: []string{"allocate"},
		Tiers: []model.PluginTier{
			{
				Plugins: []model.ConfiguredPlugin{{Name: "predicates"}},
			},
		},
	}
	sources := []source.SourceFile{
		{
			Path:        "pkg/scheduler/plugins/predicates/predicates.go",
			PluginNames: []string{"predicates"},
			Hooks:       []string{"AddPredicateFn"},
		},
	}

	reports := InspectPluginHooks(policy, sources, nil, errors.New("unsupported defaults"))
	report := findPluginHook(t, reports, "allocate", "predicates", "AddPredicateFn")

	if report.EnabledDeterminate || report.Determinate || report.Passed {
		t.Fatalf("report=%+v", report)
	}

	if !strings.Contains(report.Reason, "unsupported defaults") {
		t.Fatalf("reason=%q", report.Reason)
	}
}

func TestInspectPluginHooksDoesNotTreatExplicitNonBooleanAsOmitted(t *testing.T) {
	policy := model.SchedulerPolicy{
		Actions: []string{"allocate"},
		Tiers: []model.PluginTier{
			{
				Plugins: []model.ConfiguredPlugin{
					{
						Name:       "predicates",
						OptionKeys: []string{"enablePredicate"},
					},
				},
			},
		},
	}
	sources := []source.SourceFile{
		{
			Path:        "pkg/scheduler/plugins/predicates/predicates.go",
			PluginNames: []string{"predicates"},
			Hooks:       []string{"AddPredicateFn"},
		},
	}

	reports := InspectPluginHooks(
		policy,
		sources,
		map[string]bool{"enablePredicate": true},
		nil,
	)
	report := findPluginHook(t, reports, "allocate", "predicates", "AddPredicateFn")

	if report.EnabledDeterminate || report.Enabled || report.Determinate || report.Passed {
		t.Fatalf("report=%+v", report)
	}

	if !strings.Contains(report.Reason, "non-boolean") {
		t.Fatalf("reason=%q", report.Reason)
	}
}

func TestInspectPluginHooksMatchesVersionedNPUPluginNames(t *testing.T) {
	policy := model.SchedulerPolicy{
		Actions: []string{"allocate"},
		Tiers: []model.PluginTier{
			{
				Plugins: []model.ConfiguredPlugin{
					{Name: "volcano-npu_v6.0.RC3", Tier: 0, Order: 0},
				},
			},
		},
	}
	sources := []source.SourceFile{
		{
			Path:        "pkg/scheduler/plugins/ascend/huawei_npu.go",
			PluginNames: []string{"huaweiNPU"},
			Hooks:       []string{"AddJobValidFn"},
		},
	}

	reports := InspectPluginHooks(policy, sources, nil, nil)
	report := findPluginHook(t, reports, "allocate", "volcano-npu_v6.0.RC3", "AddJobValidFn")

	if !report.EnabledDeterminate || !report.Enabled || report.Determinate {
		t.Fatalf("report=%+v", report)
	}

	for _, item := range reports {
		if item.Hook == "source hook registration" {
			t.Fatalf("versioned NPU plugin should not be unmatched: %+v", reports)
		}
	}
}

func TestActionHookDecisionOrderCoversAllocateReclaimAndPreemptGates(t *testing.T) {
	assertHookBefore(t, "allocate", "AddQueueOrderFn", "AddOverusedFn")
	assertHookBefore(t, "allocate", "AddOverusedFn", "AddJobOrderFn")

	assertHookBefore(t, "reclaim", "AddOverusedFn", "AddPreemptiveFn")
	assertHookBefore(t, "reclaim", "AddPreemptiveFn", "AddJobOrderFn")
	assertHookOption(t, "reclaim", "AddPreemptiveFn", "enablePreemptive")

	assertHookBefore(t, "preempt", "AddJobStarvingFns", "AddJobOrderFn")
	assertHookBefore(t, "preempt", "AddAllocatableFn", "AddJobPipelinedFn")
	assertHookBefore(t, "preempt", "AddJobPipelinedFn", "AddVictimTasksFns")
	assertHookOption(t, "preempt", "AddJobStarvingFns", "enableJobStarving")
	assertHookOption(t, "preempt", "AddJobPipelinedFn", "enableJobPipelined")
	assertHookOption(t, "preempt", "AddVictimTasksFns", "enabledVictim")
}

func assertHookBefore(t *testing.T, action, first, second string) {
	t.Helper()

	firstIndex := hookDefinitionIndex(actionHooks[action], first)
	secondIndex := hookDefinitionIndex(actionHooks[action], second)
	if firstIndex < 0 || secondIndex < 0 || firstIndex >= secondIndex {
		t.Fatalf(
			"%s hook order %s=%d %s=%d: %+v",
			action,
			first,
			firstIndex,
			second,
			secondIndex,
			actionHooks[action],
		)
	}
}

func assertHookOption(t *testing.T, action, hook, option string) {
	t.Helper()

	for _, definition := range actionHooks[action] {
		if definition.name == hook {
			if definition.option != option {
				t.Fatalf("%s/%s option=%q want=%q", action, hook, definition.option, option)
			}

			return
		}
	}

	t.Fatalf("missing %s/%s", action, hook)
}

func hookDefinitionIndex(definitions []hookDefinition, hook string) int {
	for index, definition := range definitions {
		if definition.name == hook {
			return index
		}
	}

	return -1
}

func findPluginHook(
	t *testing.T,
	reports []model.PluginHookReport,
	action string,
	plugin string,
	hook string,
) model.PluginHookReport {
	t.Helper()

	for _, report := range reports {
		if report.Action == action && report.Plugin == plugin && report.Hook == hook {
			return report
		}
	}

	t.Fatalf("missing %s/%s/%s in %+v", action, plugin, hook, reports)

	return model.PluginHookReport{}
}
