package runtime

import (
	"errors"
	"reflect"
	"testing"

	"github.com/volcano-sh/volens/internal/cluster"
)

func TestObservePolicyPreservesRuntimeOrderAndSanitizesArguments(t *testing.T) {
	configuration := cluster.SchedulerRuntimeConfig{
		Determinate:              true,
		Reason:                   "observed",
		ConfigMapNamespace:       "volcano-system",
		ConfigMapName:            "volcano-scheduler-configmap",
		ConfigMapKey:             "volcano-scheduler.conf",
		ConfigMapUID:             "config-uid",
		ConfigMapResourceVersion: "42",
		Actions:                  []string{"enqueue", "allocate", "backfill"},
		Tiers: []cluster.SchedulerTier{
			{
				Plugins: []cluster.SchedulerPlugin{
					{
						Name: "predicates",
						Arguments: map[string]any{
							"credential":                   "must-not-be-copied",
							"predicate.NodeAffinityEnable": false,
						},
						Options: map[string]any{
							"enablePredicate": true,
							"customOption":    "must-not-be-copied",
						},
					},
				},
			},
		},
	}

	policy, checks := ObservePolicy(configuration, nil)

	if !reflect.DeepEqual(policy.Actions, configuration.Actions) {
		t.Fatalf("actions=%v", policy.Actions)
	}

	plugin := policy.Tiers[0].Plugins[0]
	if !reflect.DeepEqual(
		plugin.ArgumentKeys,
		[]string{"credential", "predicate.NodeAffinityEnable"},
	) {
		t.Fatalf("argument keys=%v", plugin.ArgumentKeys)
	}

	if !reflect.DeepEqual(
		plugin.ExplicitArguments,
		map[string]bool{"predicate.NodeAffinityEnable": false},
	) {
		t.Fatalf("explicit arguments=%v", plugin.ExplicitArguments)
	}

	if _, copied := plugin.ExplicitArguments["credential"]; copied {
		t.Fatalf("credential value was copied: %v", plugin.ExplicitArguments)
	}

	if !reflect.DeepEqual(plugin.ExplicitOptions, map[string]bool{"enablePredicate": true}) {
		t.Fatalf("explicit options=%v", plugin.ExplicitOptions)
	}

	if len(checks) != 2 || !checks[0].Passed || checks[1].Determinate {
		t.Fatalf("checks=%+v", checks)
	}
}

func TestObservePolicyReportsInformerFailureAsUnknown(t *testing.T) {
	_, checks := ObservePolicy(cluster.SchedulerRuntimeConfig{}, errors.New("cache unavailable"))

	if len(checks) != 1 || checks[0].Determinate || checks[0].Passed {
		t.Fatalf("checks=%+v", checks)
	}
}
