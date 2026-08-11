package runtime

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/source"
)

type hookDefinition struct {
	name   string
	option string
}

// The outer slice follows the first target-job decision path in each action.
// Inside each hook, Volcano's Session dispatches configured tiers/plugins in
// order. Queue/job/task comparators can run repeatedly while priority queues
// are built and popped, so phase is a decision-order aid, not a call count.
var actionHooks = map[string][]hookDefinition{
	"enqueue": {
		{name: "AddQueueOrderFn", option: "enableQueueOrder"},
		{name: "AddJobOrderFn", option: "enableJobOrder"},
		{name: "AddJobEnqueueableFn", option: "enableJobEnqueued"},
		{name: "AddJobEnqueuedFn", option: "enableJobEnqueued"},
	},
	"allocate": {
		{name: "AddJobValidFn"},
		{name: "AddQueueOrderFn", option: "enableQueueOrder"},
		{name: "AddOverusedFn", option: "enabledOverused"},
		{name: "AddJobOrderFn", option: "enableJobOrder"},
		{name: "AddTaskOrderFn", option: "enableTaskOrder"},
		{name: "AddAllocatableFn", option: "enabledAllocatable"},
		{name: "AddPrePredicateFn", option: "enablePredicate"},
		{name: "AddPredicateFn", option: "enablePredicate"},
		{name: "AddBatchNodeOrderFn", option: "enableNodeOrder"},
		{name: "AddNodeOrderFn", option: "enableNodeOrder"},
		{name: "AddNodeMapFn", option: "enableNodeOrder"},
		{name: "AddNodeReduceFn", option: "enableNodeOrder"},
		{name: "AddBestNodeFn", option: "enableBestNode"},
		{name: "AddJobReadyFn", option: "enableJobReady"},
		{name: "AddJobPipelinedFn", option: "enableJobPipelined"},
	},
	"backfill": {
		{name: "AddJobValidFn"},
		{name: "AddQueueOrderFn", option: "enableQueueOrder"},
		{name: "AddJobOrderFn", option: "enableJobOrder"},
		{name: "AddTaskOrderFn", option: "enableTaskOrder"},
		{name: "AddPrePredicateFn", option: "enablePredicate"},
		{name: "AddPredicateFn", option: "enablePredicate"},
		{name: "AddBatchNodeOrderFn", option: "enableNodeOrder"},
		{name: "AddNodeOrderFn", option: "enableNodeOrder"},
		{name: "AddNodeMapFn", option: "enableNodeOrder"},
		{name: "AddNodeReduceFn", option: "enableNodeOrder"},
		{name: "AddBestNodeFn", option: "enableBestNode"},
	},
	"reclaim": {
		{name: "AddJobValidFn"},
		{name: "AddQueueOrderFn", option: "enableQueueOrder"},
		{name: "AddOverusedFn", option: "enabledOverused"},
		{name: "AddPreemptiveFn", option: "enablePreemptive"},
		{name: "AddJobOrderFn", option: "enableJobOrder"},
		{name: "AddTaskOrderFn", option: "enableTaskOrder"},
		{name: "AddAllocatableFn", option: "enabledAllocatable"},
		{name: "AddPrePredicateFn", option: "enablePredicate"},
		{name: "AddPredicateFn", option: "enablePredicate"},
		{name: "AddReclaimableFn", option: "enableReclaimable"},
	},
	"preempt": {
		{name: "AddJobValidFn"},
		{name: "AddJobStarvingFns", option: "enableJobStarving"},
		{name: "AddJobOrderFn", option: "enableJobOrder"},
		{name: "AddTaskOrderFn", option: "enableTaskOrder"},
		{name: "AddPrePredicateFn", option: "enablePredicate"},
		{name: "AddPredicateFn", option: "enablePredicate"},
		{name: "AddBatchNodeOrderFn", option: "enableNodeOrder"},
		{name: "AddNodeOrderFn", option: "enableNodeOrder"},
		{name: "AddNodeMapFn", option: "enableNodeOrder"},
		{name: "AddNodeReduceFn", option: "enableNodeOrder"},
		{name: "AddPreemptableFn", option: "enablePreemptable"},
		{name: "AddAllocatableFn", option: "enabledAllocatable"},
		{name: "AddJobPipelinedFn", option: "enableJobPipelined"},
		{name: "AddVictimTasksFns", option: "enabledVictim"},
	},
}

type pluginSourceRegistration struct {
	matched bool
	hooks   map[string][]string
}

func InspectPluginHooks(
	policy model.SchedulerPolicy,
	sources []source.SourceFile,
	defaults map[string]bool,
	defaultsErr error,
) []model.PluginHookReport {
	if len(policy.Actions) == 0 || len(policy.Tiers) == 0 {
		return nil
	}

	registrations := configuredPluginRegistrations(policy, sources)
	result := make([]model.PluginHookReport, 0)

	for _, action := range policy.Actions {
		definitions, supported := actionHooks[action]
		if !supported {
			continue
		}

		unmatchedReported := map[string]bool{}

		for phase, definition := range definitions {
			for _, tier := range policy.Tiers {
				for _, plugin := range tier.Plugins {
					key := configuredPluginKey(plugin)
					registration := registrations[key]

					if !registration.matched {
						if unmatchedReported[key] {
							continue
						}

						unmatchedReported[key] = true
						result = append(result, model.PluginHookReport{
							Action:      action,
							Phase:       phase,
							Tier:        plugin.Tier,
							Order:       plugin.Order,
							Plugin:      plugin.Name,
							Hook:        "source hook registration",
							Passed:      false,
							Determinate: false,
							Reason:      "the configured plugin is not present in the selected branch hook index; the chosen branch may not match the scheduler image or the plugin may be delivered outside this Volcano source tree",
						})

						continue
					}

					sourcePaths, registered := registration.hooks[definition.name]
					if !registered {
						continue
					}

					result = append(result, pluginHookReport(
						action,
						phase,
						plugin,
						definition,
						sourcePaths,
						defaults,
						defaultsErr,
					))
				}
			}
		}
	}

	return result
}

func pluginHookReport(
	action string,
	phase int,
	plugin model.ConfiguredPlugin,
	definition hookDefinition,
	sourcePaths []string,
	defaults map[string]bool,
	defaultsErr error,
) model.PluginHookReport {
	enabled, enabledDeterminate, enabledReason := effectiveHookOption(
		plugin,
		definition,
		defaults,
		defaultsErr,
	)

	report := model.PluginHookReport{
		Action:             action,
		Phase:              phase,
		Tier:               plugin.Tier,
		Order:              plugin.Order,
		Plugin:             plugin.Name,
		Hook:               definition.name,
		Enabled:            enabled,
		EnabledDeterminate: enabledDeterminate,
		Passed:             false,
		Determinate:        false,
		Reason:             enabledReason,
		Source:             append([]string(nil), sourcePaths...),
	}

	switch {
	case !enabledDeterminate:
		report.Reason += "; runtime execution cannot be decided"
	case !enabled:
		report.Passed = true
		report.Determinate = true
		report.Reason += "; the hook is skipped"
	default:
		report.Reason += "; the hook is registered and enabled, but its result depends on scheduler Session state and plugin logic"
	}

	return report
}

func effectiveHookOption(
	plugin model.ConfiguredPlugin,
	definition hookDefinition,
	defaults map[string]bool,
	defaultsErr error,
) (bool, bool, string) {
	if definition.option == "" {
		return true, true, "this hook has no PluginOption enable switch in the Volcano framework"
	}

	if value, found := plugin.ExplicitOptions[definition.option]; found {
		return value, true, fmt.Sprintf(
			"ConfigMap explicitly sets %s=%t",
			definition.option,
			value,
		)
	}

	if containsString(plugin.OptionKeys, definition.option) {
		return false, false, fmt.Sprintf(
			"ConfigMap sets %s to a non-boolean value; the scheduler parser outcome cannot be inferred safely",
			definition.option,
		)
	}

	if defaultsErr != nil {
		return false, false, fmt.Sprintf(
			"ConfigMap omits %s and selected-branch defaults could not be parsed: %v",
			definition.option,
			defaultsErr,
		)
	}

	if value, found := defaults[definition.option]; found {
		return value, true, fmt.Sprintf(
			"ConfigMap omits %s; selected-branch ApplyPluginConfDefaults sets it to %t",
			definition.option,
			value,
		)
	}

	return false, false, fmt.Sprintf(
		"ConfigMap omits %s and the selected branch does not expose a reliably parsed default",
		definition.option,
	)
}

func configuredPluginRegistrations(
	policy model.SchedulerPolicy,
	sources []source.SourceFile,
) map[string]pluginSourceRegistration {
	result := map[string]pluginSourceRegistration{}

	for _, tier := range policy.Tiers {
		for _, plugin := range tier.Plugins {
			result[configuredPluginKey(plugin)] = pluginRegistration(plugin.Name, sources)
		}
	}

	return result
}

func configuredPluginKey(plugin model.ConfiguredPlugin) string {
	return fmt.Sprintf("%d/%d/%s", plugin.Tier, plugin.Order, plugin.Name)
}

func pluginRegistration(name string, sources []source.SourceFile) pluginSourceRegistration {
	registration := pluginSourceRegistration{hooks: map[string][]string{}}

	for _, file := range sources {
		if !sourceBelongsToPlugin(file, name) {
			continue
		}

		registration.matched = true

		for _, hook := range file.Hooks {
			registration.hooks[hook] = append(registration.hooks[hook], file.Path)
		}
	}

	for hook := range registration.hooks {
		sort.Strings(registration.hooks[hook])
	}

	return registration
}

func sourceBelongsToPlugin(file source.SourceFile, pluginName string) bool {
	for _, name := range file.PluginNames {
		if pluginNamesMatch(name, pluginName) {
			return true
		}
	}

	if len(file.PluginNames) > 0 || len(file.Hooks) == 0 {
		return false
	}

	const prefix = "pkg/scheduler/plugins/"
	relative := strings.TrimPrefix(filepath.ToSlash(file.Path), prefix)
	directory := filepath.ToSlash(filepath.Dir(relative))
	parts := strings.Split(directory, "/")

	if len(parts) == 1 {
		return pluginNamesMatch(parts[0], pluginName)
	}

	return pluginNamesMatch(parts[len(parts)-1], pluginName)
}

func pluginNamesMatch(sourceName, configuredName string) bool {
	sourceNames := pluginNameCandidates(sourceName)
	configuredNames := pluginNameCandidates(configuredName)

	for _, left := range sourceNames {
		for _, right := range configuredNames {
			if left == right {
				return true
			}
		}
	}

	return false
}

func pluginNameCandidates(name string) []string {
	normalized := normalizePluginName(name)
	candidates := []string{normalized}

	for _, separator := range []string{"_v", "-v"} {
		if index := strings.LastIndex(normalized, separator); index > 0 {
			candidates = append(candidates, normalized[:index])
		}
	}

	for _, candidate := range append([]string(nil), candidates...) {
		if strings.HasPrefix(candidate, "volcano-") {
			candidates = append(candidates, strings.TrimPrefix(candidate, "volcano-"))
		}

		if strings.HasSuffix(candidate, "npu") {
			candidates = append(candidates, "ascend", "huaweinpu")
		}
	}

	seen := map[string]bool{}
	result := make([]string, 0, len(candidates))

	for _, candidate := range candidates {
		if candidate == "" || seen[candidate] {
			continue
		}

		seen[candidate] = true
		result = append(result, candidate)
	}

	return result
}

func normalizePluginName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "-")
	name = strings.ReplaceAll(name, ".", "")

	return name
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}

	return false
}
