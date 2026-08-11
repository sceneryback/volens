package runtime

import (
	"fmt"
	"sort"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
)

var schedulerPolicySources = []string{
	"pkg/scheduler/util.go:UnmarshalSchedulerConf",
	"pkg/scheduler/scheduler.go:runOnce",
	"pkg/scheduler/framework/framework.go:OpenSession",
}

func ObservePolicy(
	configuration cluster.SchedulerRuntimeConfig,
	configurationErr error,
) (model.SchedulerPolicy, []model.Check) {
	policy := schedulerPolicy(configuration)

	if configurationErr != nil {
		return policy, []model.Check{
			model.Unknown(
				"scheduler.policy.observed",
				"preflight",
				"Scheduler policy loaded from informer cache",
				configurationErr.Error(),
				nil,
				schedulerPolicySources,
			),
		}
	}

	if !configuration.Determinate {
		return policy, []model.Check{
			model.Unknown(
				"scheduler.policy.observed",
				"preflight",
				"Scheduler policy loaded from informer cache",
				configuration.Reason,
				policy,
				schedulerPolicySources,
			),
		}
	}

	observed := model.Known(
		"scheduler.policy.observed",
		"preflight",
		"Mounted scheduler ConfigMap parsed",
		true,
		fmt.Sprintf(
			"observed %s/%s key=%s uid=%s resourceVersion=%s; actions=%v",
			configuration.ConfigMapNamespace,
			configuration.ConfigMapName,
			configuration.ConfigMapKey,
			configuration.ConfigMapUID,
			configuration.ConfigMapResourceVersion,
			configuration.Actions,
		),
		schedulerPolicySources,
	)
	observed.Evidence = policy

	active := model.Unknown(
		"scheduler.policy.active",
		"preflight",
		"Observed scheduler policy is active in memory",
		"the ConfigMap informer proves the mounted document version, but volume projection and scheduler fsnotify reload are eventually consistent; scheduler reload logs are required to prove the in-memory Session uses it",
		map[string]string{
			"configMapUID":             configuration.ConfigMapUID,
			"configMapResourceVersion": configuration.ConfigMapResourceVersion,
		},
		schedulerPolicySources,
	)

	return policy, []model.Check{observed, active}
}

func schedulerPolicy(configuration cluster.SchedulerRuntimeConfig) model.SchedulerPolicy {
	policy := model.SchedulerPolicy{
		Determinate:              configuration.Determinate,
		ActiveDeterminate:        false,
		Reason:                   configuration.Reason,
		SchedulerNamespace:       configuration.SchedulerNamespace,
		SchedulerPod:             configuration.SchedulerPod,
		SchedulerUID:             configuration.SchedulerUID,
		SchedulerConfPath:        configuration.SchedulerConfPath,
		ConfigMapNamespace:       configuration.ConfigMapNamespace,
		ConfigMapName:            configuration.ConfigMapName,
		ConfigMapKey:             configuration.ConfigMapKey,
		ConfigMapUID:             configuration.ConfigMapUID,
		ConfigMapResourceVersion: configuration.ConfigMapResourceVersion,
		Actions:                  append([]string(nil), configuration.Actions...),
		Tiers:                    make([]model.PluginTier, 0, len(configuration.Tiers)),
	}

	for tierIndex, configuredTier := range configuration.Tiers {
		tier := model.PluginTier{
			Order:   tierIndex,
			Plugins: make([]model.ConfiguredPlugin, 0, len(configuredTier.Plugins)),
		}

		for pluginIndex, configuredPlugin := range configuredTier.Plugins {
			plugin := model.ConfiguredPlugin{
				Name:              configuredPlugin.Name,
				Tier:              tierIndex,
				Order:             pluginIndex,
				ArgumentKeys:      sortedKeys(configuredPlugin.Arguments),
				OptionKeys:        sortedKeys(configuredPlugin.Options),
				ExplicitArguments: booleanOptions(configuredPlugin.Arguments),
				ExplicitOptions:   booleanOptions(configuredPlugin.Options),
			}

			tier.Plugins = append(tier.Plugins, plugin)
		}

		policy.Tiers = append(policy.Tiers, tier)
	}

	return policy
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))

	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

func booleanOptions(values map[string]any) map[string]bool {
	options := make(map[string]bool)

	for key, value := range values {
		boolean, ok := value.(bool)
		if ok {
			options[key] = boolean
		}
	}

	if len(options) == 0 {
		return nil
	}

	return options
}
