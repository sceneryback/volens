package agent

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
)

type presentationEvidence struct {
	Pod               *corev1.Pod
	Tasks             []corev1.Pod
	PodGroup          cluster.PodGroup
	QueueName         string
	Queue             cluster.Queue
	QueueErr          error
	QueueSnapshot     cluster.QueueSnapshot
	QueueSnapshotErr  error
	QueuePodGroups    []cluster.PodGroup
	QueuePodGroupsErr error
	PreflightStopped  bool
	EnqueueStopped    bool
	JobValidStopped   bool
	AllocateStopped   bool
	StopReason        string
}

func buildPresentation(report *Report, evidence presentationEvidence) {
	report.Enqueue = buildEnqueuePresentation(*report, evidence)
	report.JobValid = buildJobValidPresentation(*report, evidence)
	report.Allocate = buildAllocatePresentation(*report, evidence)
	report.Diagnosis = diagnoseReport(*report)
}

func synchronizePresentation(report *Report) {
	if report.Allocate.State.Outcome == model.OutcomeSkipped {
		return
	}

	report.Nodes = cloneNodeResults(report.Allocate.Nodes)
	report.Checks = replaceCheck(
		report.Checks,
		"allocate.nodes",
		nodeAvailabilityCheck(report.Nodes),
	)
}

func cloneNodeResults(nodes []NodeResult) []NodeResult {
	result := make([]NodeResult, 0, len(nodes))

	for _, node := range nodes {
		clone := node
		clone.Checks = cloneChecks(node.Checks)
		clone.Resources = make(map[string]ResourcePair, len(node.Resources))

		for name, pair := range node.Resources {
			clone.Resources[name] = ResourcePair{
				Free:         copyFloat(pair.Free),
				Total:        copyFloat(pair.Total),
				Used:         copyFloat(pair.Used),
				Releasing:    copyFloat(pair.Releasing),
				Insufficient: pair.Insufficient,
			}
		}

		result = append(result, clone)
	}

	return result
}

func buildEnqueuePresentation(
	report Report,
	evidence presentationEvidence,
) EnqueueReport {
	checks := checksForStage(report.Checks, "enqueue")

	if evidence.PreflightStopped {
		reason := evidence.StopReason
		if reason == "" {
			reason = "preflight did not pass, so Volcano does not reach enqueue"
		}

		return EnqueueReport{
			State: StageState{
				Outcome:    model.OutcomeSkipped,
				Conclusion: "未执行入队检查",
				SkipReason: reason,
			},
			Checks: skippedChecks(checks, "enqueue", reason),
		}
	}

	actions := map[string]bool{"enqueue": true}
	hookChecks := pluginHookChecks(
		report.PluginHooks,
		actions,
		map[string]bool{"AddJobEnqueueableFn": true},
		"enqueue",
	)
	// proportion and capacity are evaluated below from the same cache snapshot
	// with their exact JobEnqueueable quota formula. Do not render a second,
	// generic hook column for the same decision.
	hookChecks = removePluginCheck(hookChecks, "proportion")
	hookChecks = removePluginCheck(hookChecks, "capacity")
	unmatchedChecks := unmatchedPluginChecks(
		report.PluginHooks,
		actions,
		"enqueue",
		"JobEnqueueable",
	)
	inventoryVerified := hookInventoryVerified(report)
	standaloneEnqueue := policyHasAction(report.Policy, "enqueue")

	if evidence.PodGroup.MinResources == nil {
		hookChecks = skippedChecks(
			hookChecks,
			"enqueue",
			"PodGroup spec.minResources is nil, so the standard enqueue action bypasses JobEnqueueable",
		)
		unmatchedChecks = skippedChecks(
			unmatchedChecks,
			"enqueue",
			"PodGroup spec.minResources is nil, so enqueue bypasses JobEnqueueable",
		)
	}

	if inventoryVerified && standaloneEnqueue {
		checks = removeCheck(checks, "plugins.job-enqueueable")

		if len(hookChecks) == 0 && len(unmatchedChecks) == 0 {
			checks = append(checks, model.Known(
				"plugins.job-enqueueable.none",
				"enqueue",
				"No active JobEnqueueable hooks",
				true,
				"the selected branch proves that no configured plugin registers JobEnqueueable for the active actions",
				nil,
			))
		}
	}

	checks = append(checks, hookChecks...)
	checks = append(checks, unmatchedChecks...)

	queue := QueueSummary{
		Name:      evidence.QueueName,
		State:     evidence.Queue.State,
		Strategy:  queueStrategy(report.Policy),
		Resources: queueResourceValues(evidence.QueueSnapshot, evidence.PodGroup),
	}
	queue.Formula = "need = MinReq + allocated + inqueue - elastic; permit iff need <= realCapability"
	queue.FormulaNote = queueStrategyNote(queue.Strategy)

	if evidence.QueueSnapshotErr != nil {
		queue.RuntimeReason = evidence.QueueSnapshotErr.Error()
	} else if len(evidence.QueueSnapshot.Resources) == 0 {
		queue.RuntimeReason = "scheduler cache dump did not expose queue runtime resources"
	} else {
		queue.RuntimeDeterminate = true
		queue.RuntimeSource = evidence.QueueSnapshot.Source
		queue.RuntimeReason = "values were reconstructed from the active scheduler cache dump"
	}

	checks = append(checks, queueRuntimeCheck(queue, evidence.QueueSnapshotErr))
	checks = append(checks, queuePodGroupsCheck(evidence.QueuePodGroups, evidence.QueuePodGroupsErr))

	if evidence.PodGroup.MinResources != nil {
		checks = append(checks, queueCapacityCheck(
			queue,
			evidence.QueueSnapshotErr,
			report.Policy.Determinate,
		))
	}

	return EnqueueReport{
		State:     enqueueState(checks),
		Queue:     queue,
		Checks:    checks,
		PodGroups: podGroupSummaries(evidence.QueuePodGroups, evidence.PodGroup),
	}
}

func policyHasAction(policy SchedulerPolicy, wanted string) bool {
	for _, action := range policy.Actions {
		if action == wanted {
			return true
		}
	}

	return false
}

func enqueueState(checks []Check) StageState {
	for _, check := range checks {
		if check.Determinate && !check.Passed && !check.Skipped {
			return stateFromChecks(checks)
		}
	}

	for _, check := range checks {
		if check.ID == "job.enqueue.evidence" && check.Determinate && check.Passed {
			return StageState{
				Outcome: model.OutcomePass,
				Conclusion: "PodGroup 已有确定入队证据；问号表示无法逐项回溯插件投票，" +
					"不改变该阶段已经通过的事实",
			}
		}
	}

	return stateFromChecks(checks)
}

func queuePodGroupsCheck(groups []cluster.PodGroup, err error) Check {
	if err != nil {
		return model.Unknown(
			"queue.podgroups",
			"enqueue",
			"Queue PodGroup snapshot",
			err.Error(),
			nil,
			[]string{"PodGroup informer queue index"},
		)
	}

	return model.Known(
		"queue.podgroups",
		"enqueue",
		"Queue PodGroup snapshot",
		true,
		fmt.Sprintf("loaded %d PodGroups from the informer cache", len(groups)),
		[]string{"PodGroup informer queue index"},
	)
}

func buildJobValidPresentation(
	report Report,
	evidence presentationEvidence,
) JobValidReport {
	checks := checksForStage(report.Checks, "jobValid")
	actions := map[string]bool{
		"allocate": true,
		"backfill": true,
		"preempt":  true,
		"reclaim":  true,
	}
	hookChecks := pluginHookChecks(
		report.PluginHooks,
		actions,
		map[string]bool{"AddJobValidFn": true},
		"jobValid",
	)
	unmatchedChecks := unmatchedPluginChecks(
		report.PluginHooks,
		actions,
		"jobValid",
		"JobValid",
	)
	inventoryVerified := hookInventoryVerified(report)
	gangActive := report.Policy.ActiveDeterminate &&
		hasEnabledHookPlugin(report.PluginHooks, "AddJobValidFn", "gang")
	if gangActive {
		checks = resolveGangChecks(checks)
		hookChecks = removePluginCheck(hookChecks, "gang")
	}

	if inventoryVerified {
		checks = removeCheck(checks, "plugins.job-valid")

		if !hasHookPlugin(report.PluginHooks, "AddJobValidFn", "gang") {
			checks = removeCheckPrefix(checks, "plugins.gang.")
		}

		if len(hookChecks) == 0 && len(unmatchedChecks) == 0 && !gangActive {
			checks = append(checks, model.Known(
				"plugins.job-valid.none",
				"jobValid",
				"No active JobValid hooks",
				true,
				"the selected branch proves that no configured plugin registers JobValid for the active actions",
				nil,
			))
		}
	}

	checks = append(checks, hookChecks...)
	checks = append(checks, unmatchedChecks...)

	if evidence.PreflightStopped || evidence.EnqueueStopped || evidence.JobValidStopped {
		reason := evidence.StopReason
		if reason == "" {
			reason = "enqueue did not pass, so Volcano does not reach allocate JobValid"
		}

		checks = skippedChecks(checks, "jobValid", reason)

		return JobValidReport{
			State: StageState{
				Outcome:    model.OutcomeSkipped,
				Conclusion: "未执行 JobValid",
				SkipReason: reason,
			},
			Rows: workloadRows(evidence.Pod, evidence.Tasks, checks),
		}
	}

	return JobValidReport{
		State: stateFromChecks(checks),
		Rows:  workloadRows(evidence.Pod, evidence.Tasks, checks),
	}
}

func resolveGangChecks(checks []Check) []Check {
	result := make([]Check, 0, len(checks))

	for _, check := range checks {
		if !strings.HasPrefix(check.ID, "plugins.gang.") {
			result = append(result, check)

			continue
		}

		evidence, ok := check.Evidence.(map[string]any)
		if !ok {
			result = append(result, check)

			continue
		}

		wouldPass, ok := evidence["wouldPass"].(bool)
		if !ok {
			result = append(result, check)

			continue
		}

		if wouldPass {
			resolved := model.Known(
				check.ID,
				"jobValid",
				check.Name,
				true,
				"Kubernetes-visible valid tasks alone satisfy this gang minimum; "+
					"Volcano internal Pipelined/Allocated/Binding states can only add valid tasks",
				check.Source,
			)
			resolved.Evidence = check.Evidence
			result = append(result, resolved)

			continue
		}

		unknown := check
		unknown.Reason += "; Volcano internal Pipelined/Allocated/Binding task states are absent, so failure is not proven"
		result = append(result, unknown)
	}

	return result
}

func buildAllocatePresentation(
	report Report,
	evidence presentationEvidence,
) AllocateReport {
	if evidence.PreflightStopped || evidence.EnqueueStopped ||
		evidence.JobValidStopped || evidence.AllocateStopped {
		reason := evidence.StopReason
		if reason == "" {
			reason = "a previous scheduler gate did not pass"
		}

		return AllocateReport{
			State: StageState{
				Outcome:    model.OutcomeSkipped,
				Conclusion: "未执行节点分配检查",
				SkipReason: reason,
			},
		}
	}

	dynamicChecks := pluginHookChecks(
		report.PluginHooks,
		map[string]bool{"allocate": true, "backfill": true},
		map[string]bool{"AddPrePredicateFn": true, "AddPredicateFn": true},
		"allocate",
	)
	hasDynamicHooks := len(dynamicChecks) > 0
	dynamicChecks = removePluginCheck(dynamicChecks, "predicates")
	nodes := append([]NodeResult(nil), report.Nodes...)
	predicateMode, predicateReason := predicatesActivation(report)

	for index := range nodes {
		nodes[index].Checks = normalizePredicateChecks(
			nodes[index].Checks,
			report,
			predicateMode,
			predicateReason,
		)
		nodes[index].Checks = mergeChecks(nodes[index].Checks, dynamicChecks)
		recomputeNodeOutcome(&nodes[index])
	}

	checks := checksForStage(report.Checks, "allocate")
	checks = removeCheck(checks, "allocate.nodes")
	checks = append(checks, nodeAvailabilityCheck(nodes))
	checks = append(checks, dynamicChecks...)

	if hasDynamicHooks {
		checks = removeCheck(checks, "plugins.predicates")
	}

	return AllocateReport{
		State: stateFromChecks(checks),
		Nodes: nodes,
	}
}

type pluginActivation int

const (
	pluginActivationUnknown pluginActivation = iota
	pluginActivationDisabled
	pluginActivationEnabled
)

var predicateCheckIDs = map[string]bool{
	"node.selector":        true,
	"node.affinity":        true,
	"node.schedulable":     true,
	"node.taints":          true,
	"node.pod-count":       true,
	"node.ports":           true,
	"node.pod-affinity":    true,
	"node.volume-limits":   true,
	"node.volume-zone":     true,
	"node.topology-spread": true,
	"node.proportional":    true,
}

func predicatesActivation(report Report) (pluginActivation, string) {
	if !report.Policy.ActiveDeterminate {
		return pluginActivationUnknown,
			"ConfigMap 已读取，但尚未从 scheduler 日志确认该版本已加载到当前 Session (not proven active)；以下仅是本地规则投影"
	}

	configured := false

	for _, tier := range report.Policy.Tiers {
		for _, plugin := range tier.Plugins {
			if strings.EqualFold(plugin.Name, "predicates") {
				configured = true
			}
		}
	}

	if !configured {
		return pluginActivationDisabled,
			"predicates is not configured in the active scheduler plugin tiers"
	}

	if !report.HooksInspected {
		return pluginActivationUnknown,
			"所选分支的插件注册源码未完成索引"
	}

	found := false
	unknown := false
	disabled := false

	for _, hook := range report.PluginHooks {
		if !strings.EqualFold(hook.Plugin, "predicates") ||
			(hook.Hook != "AddPrePredicateFn" && hook.Hook != "AddPredicateFn") ||
			(hook.Action != "allocate" && hook.Action != "backfill") {
			continue
		}

		found = true

		switch {
		case !hook.EnabledDeterminate:
			unknown = true
		case hook.Enabled:
			return pluginActivationEnabled, hook.Reason
		default:
			disabled = true
		}
	}

	if unknown {
		return pluginActivationUnknown,
			"predicates enablement cannot be determined from the active ConfigMap and selected branch defaults"
	}

	if found && disabled {
		return pluginActivationDisabled,
			"all discovered predicates hooks are disabled by the active plugin configuration"
	}

	return pluginActivationUnknown,
		"ConfigMap 配置了 predicates，但所选分支源码没有匹配到 Predicate 注册；分支可能与 scheduler 镜像不一致"
}

var predicateArgumentByCheck = map[string]string{
	"node.selector":        "predicate.NodeAffinityEnable",
	"node.affinity":        "predicate.NodeAffinityEnable",
	"node.taints":          "predicate.TaintTolerationEnable",
	"node.ports":           "predicate.NodePortsEnable",
	"node.pod-affinity":    "predicate.PodAffinityEnable",
	"node.volume-limits":   "predicate.NodeVolumeLimitsEnable",
	"node.volume-zone":     "predicate.VolumeZoneEnable",
	"node.topology-spread": "predicate.PodTopologySpreadEnable",
	"node.proportional":    "predicate.ProportionalEnable",
}

func predicateCheckActivation(report Report, checkID string) (pluginActivation, string) {
	argument, hasSwitch := predicateArgumentByCheck[checkID]
	if !hasSwitch {
		return pluginActivationEnabled,
			"the predicates hook always runs this check when the plugin is enabled"
	}

	foundPlugin := false
	unknown := false
	disabled := false

	for _, tier := range report.Policy.Tiers {
		for _, plugin := range tier.Plugins {
			if !strings.EqualFold(plugin.Name, "predicates") {
				continue
			}

			foundPlugin = true

			if value, found := plugin.ExplicitArguments[argument]; found {
				if value {
					return pluginActivationEnabled,
						fmt.Sprintf("ConfigMap explicitly sets %s=true", argument)
				}

				disabled = true

				continue
			}

			if containsValue(plugin.ArgumentKeys, argument) {
				unknown = true

				continue
			}

			if value, found := report.PredicateDefaults[argument]; found {
				if value {
					return pluginActivationEnabled,
						fmt.Sprintf("selected branch defaults %s=true", argument)
				}

				disabled = true

				continue
			}

			unknown = true
		}
	}

	if !foundPlugin {
		return pluginActivationDisabled,
			"predicates is not configured in the active scheduler plugin tiers"
	}

	if unknown {
		reason := "the effective predicates argument " + argument + " is unknown"
		if report.PredicateDefaultsErr != "" {
			reason += ": " + report.PredicateDefaultsErr
		}

		return pluginActivationUnknown, reason
	}

	if disabled {
		return pluginActivationDisabled,
			fmt.Sprintf("ConfigMap or selected branch default sets %s=false", argument)
	}

	return pluginActivationUnknown,
		"the effective predicates argument " + argument + " is unavailable"
}

func containsValue(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}

	return false
}

func normalizePredicateChecks(
	checks []Check,
	report Report,
	activation pluginActivation,
	reason string,
) []Check {
	result := make([]Check, 0, len(checks))

	for _, check := range checks {
		if !predicateCheckIDs[check.ID] {
			result = append(result, check)

			continue
		}

		checkActivation := activation
		checkReason := reason

		if activation == pluginActivationEnabled {
			checkActivation, checkReason = predicateCheckActivation(report, check.ID)
		}

		if checkActivation == pluginActivationEnabled {
			if check.Skipped {
				result = append(result, model.Unknown(
					check.ID,
					"allocate",
					check.Name,
					checkReason+"; this enabled optional predicate needs its branch-specific arguments and exact Session state",
					check.Evidence,
					check.Source,
				))

				continue
			}

			result = append(result, check)

			continue
		}

		switch checkActivation {
		case pluginActivationDisabled:
			skipped := model.Skipped(
				check.ID,
				"allocate",
				check.Name,
				checkReason,
				check.Source,
			)
			skipped.Evidence = check.Evidence
			result = append(result, skipped)

		default:
			if check.Skipped && !predicateMayBeEnabled(check.ID, report) {
				result = append(result, check)

				continue
			}

			if check.Determinate && !check.Skipped {
				check.Reason = check.Reason + "; local Kubernetes-visible projection, scheduler ConfigMap activation is not proven but this result is directly computed from Pod/Node objects"
				result = append(result, check)

				continue
			}

			reason := checkReason + "; local projection still needs scheduler Session evidence"

			unknown := model.Unknown(
				check.ID,
				"allocate",
				check.Name,
				reason,
				check.Evidence,
				check.Source,
			)
			result = append(result, unknown)
		}
	}

	return result
}

func predicateMayBeEnabled(checkID string, report Report) bool {
	argument, hasSwitch := predicateArgumentByCheck[checkID]
	if !hasSwitch {
		return true
	}

	for _, tier := range report.Policy.Tiers {
		for _, plugin := range tier.Plugins {
			if !strings.EqualFold(plugin.Name, "predicates") {
				continue
			}

			if value, found := plugin.ExplicitArguments[argument]; found {
				return value
			}

			if containsValue(plugin.ArgumentKeys, argument) {
				return true
			}
		}
	}

	value, found := report.PredicateDefaults[argument]

	return found && value
}

func mergeChecks(existing []Check, additions []Check) []Check {
	result := append([]Check(nil), existing...)
	indexes := make(map[string]int, len(result))

	for index, check := range result {
		indexes[check.ID] = index
	}

	for _, check := range additions {
		if index, found := indexes[check.ID]; found {
			result[index] = check

			continue
		}

		indexes[check.ID] = len(result)
		result = append(result, check)
	}

	return result
}

func nodeAvailabilityCheck(nodes []NodeResult) Check {
	passed := 0
	unknown := 0

	for _, node := range nodes {
		if node.Passed {
			passed++
		} else if !node.Determinate {
			unknown++
		}
	}

	reason := fmt.Sprintf(
		"%d/%d nodes passed all active, reproducible filters; %d node results remain unknown",
		passed,
		len(nodes),
		unknown,
	)
	sources := []string{
		"pkg/scheduler/actions/allocate/allocate.go",
		"pkg/scheduler/framework/session_plugins.go:PredicateFn",
	}

	if passed > 0 {
		return model.Known(
			"allocate.nodes",
			"allocate",
			"At least one node passes active filters",
			true,
			reason,
			sources,
		)
	}

	if unknown > 0 {
		return model.Unknown(
			"allocate.nodes",
			"allocate",
			"At least one node passes active filters",
			reason,
			nil,
			sources,
		)
	}

	return model.Known(
		"allocate.nodes",
		"allocate",
		"At least one node passes active filters",
		false,
		reason,
		sources,
	)
}

func removePluginCheck(checks []Check, plugin string) []Check {
	prefix := "plugin." + identifier(plugin) + "."
	result := make([]Check, 0, len(checks))

	for _, check := range checks {
		if !strings.HasPrefix(check.ID, prefix) {
			result = append(result, check)
		}
	}

	return result
}

func queueResourceValues(
	snapshot cluster.QueueSnapshot,
	group cluster.PodGroup,
) map[string]QueueResourceValue {
	result := make(map[string]QueueResourceValue, len(snapshot.Resources))
	names := make([]string, 0, len(snapshot.Resources))

	for name := range snapshot.Resources {
		names = append(names, name)
	}

	sort.SliceStable(names, func(left, right int) bool {
		leftRequest := strings.HasPrefix(strings.ToLower(names[left]), "requests.")
		rightRequest := strings.HasPrefix(strings.ToLower(names[right]), "requests.")

		if leftRequest != rightRequest {
			return !leftRequest
		}

		return names[left] < names[right]
	})

	for _, originalName := range names {
		value := snapshot.Resources[originalName]
		name := canonicalResourceName(originalName)
		zero := 0.0
		if value.Capability != nil {
			if value.Allocated == nil {
				value.Allocated = &zero
			}

			if value.Inqueue == nil {
				value.Inqueue = &zero
			}

			if value.Elastic == nil {
				value.Elastic = &zero
			}

			if value.Request == nil {
				value.Request = &zero
			}
		}

		merged := result[name]
		mergeQueueResourceValue(&merged, value)
		result[name] = merged
	}

	if group.MinResources == nil {
		return result
	}

	for name, candidate := range resourceListValues(group.MinResources) {
		value := result[name]
		candidateCopy := candidate
		value.Candidate = &candidateCopy

		if value.Capability != nil && value.Allocated != nil && value.Inqueue != nil {
			available := *value.Capability - *value.Allocated - *value.Inqueue
			required := candidate + *value.Allocated + *value.Inqueue
			if value.Elastic != nil {
				available += *value.Elastic
				required -= *value.Elastic
			}
			value.Available = &available
			value.Required = &required
		}

		result[name] = value
	}

	return result
}

func mergeQueueResourceValue(destination *QueueResourceValue, source cluster.QueueSnapshotResource) {
	if source.Capability != nil {
		destination.Capability = copyFloat(source.Capability)
	}
	if source.Deserved != nil {
		destination.Deserved = copyFloat(source.Deserved)
	}
	if source.Allocated != nil {
		destination.Allocated = copyFloat(source.Allocated)
	}
	if source.Request != nil {
		destination.Request = copyFloat(source.Request)
	}
	if source.Inqueue != nil {
		destination.Inqueue = copyFloat(source.Inqueue)
	}
	if source.Elastic != nil {
		destination.Elastic = copyFloat(source.Elastic)
	}
}

func queueRuntimeCheck(queue QueueSummary, err error) Check {
	if err != nil {
		return model.Unknown(
			"queue.runtime-snapshot",
			"enqueue",
			"Queue runtime resources",
			err.Error(),
			nil,
			[]string{"SIGUSR2 cache dump", "pkg/scheduler/cache/dumper.go"},
		)
	}

	if !queue.RuntimeDeterminate {
		return model.Unknown(
			"queue.runtime-snapshot",
			"enqueue",
			"Queue runtime resources",
			queue.RuntimeReason,
			nil,
			[]string{"SIGUSR2 cache dump", "pkg/scheduler/cache/dumper.go"},
		)
	}

	return model.Known(
		"queue.runtime-snapshot",
		"enqueue",
		"Queue runtime resources",
		true,
		queue.RuntimeReason,
		[]string{"SIGUSR2 cache dump", "pkg/scheduler/cache/dumper.go"},
	)
}

func queueCapacityCheck(
	queue QueueSummary,
	snapshotErr error,
	configurationDeterminate bool,
) Check {
	strategy := queueBaseStrategy(queue.Strategy)
	sources := queueStrategySources(strategy)
	checkName := "Queue JobEnqueueable quota formula"
	if strategy != "" {
		checkName = strategy + " JobEnqueueable quota formula"
	}

	if snapshotErr != nil {
		return model.Unknown(
			"queue.enqueue-capacity",
			"enqueue",
			checkName,
			"the local scheduler cache snapshot was unavailable: "+snapshotErr.Error(),
			queue.Resources,
			sources,
		)
	}

	if !configurationDeterminate {
		return model.Unknown(
			"queue.enqueue-capacity",
			"enqueue",
			checkName,
			"the scheduler ConfigMap could not be parsed deterministically, so the active queue plugin cannot be selected",
			queue.Resources,
			sources,
		)
	}

	if strategy == "" {
		return model.Skipped(
			"queue.enqueue-capacity",
			"enqueue",
			checkName,
			"neither proportion nor capacity is enabled in the scheduler plugin tiers",
			sources,
		)
	}

	missing := make([]string, 0)
	uncertain := make([]string, 0)

	for name, value := range queue.Resources {
		if value.Candidate == nil || *value.Candidate <= 0 {
			continue
		}

		if value.Available == nil {
			missing = append(missing, name)

			continue
		}

		if *value.Candidate > *value.Available {
			uncertain = append(uncertain, fmt.Sprintf(
				"%s candidate %.3f > conservative available %.3f",
				name,
				*value.Candidate,
				*value.Available,
			))
		}
	}

	sort.Strings(missing)
	sort.Strings(uncertain)

	if len(missing) > 0 {
		return model.Unknown(
			"queue.enqueue-capacity",
			"enqueue",
			checkName,
			"local cache reconstruction is missing candidate dimensions: "+strings.Join(missing, ", "),
			queue.Resources,
			sources,
		)
	}

	if len(uncertain) > 0 {
		return model.Known(
			"queue.enqueue-capacity",
			"enqueue",
			checkName,
			false,
			strings.Join(uncertain, "; ")+"; need=MinReq+allocated+inqueue-elastic exceeds realCapability",
			sources,
		)
	}

	return model.Known(
		"queue.enqueue-capacity",
		"enqueue",
		checkName,
		true,
		"need=MinReq+allocated+inqueue-elastic is <= realCapability in every requested dimension",
		sources,
	)
}

func queueStrategy(policy SchedulerPolicy) string {
	strategies := map[string]bool{}
	configured := false

	for _, tier := range policy.Tiers {
		for _, plugin := range tier.Plugins {
			name := strings.ToLower(plugin.Name)

			switch name {
			case "proportion", "capacity":
				configured = true
				if enabled, explicitlySet := plugin.ExplicitOptions["enableJobEnqueued"]; explicitlySet && !enabled {
					continue
				}

				strategies[name] = true
			}
		}
	}

	ordered := make([]string, 0, len(strategies))

	for name := range strategies {
		ordered = append(ordered, name)
	}

	sort.Strings(ordered)

	strategy := strings.Join(ordered, " + ")
	if strategy == "" && configured {
		return "disabled"
	}

	return strategy
}

func queueBaseStrategy(strategy string) string {
	if strings.Contains(strategy, "capacity") {
		return "capacity"
	}
	if strings.Contains(strategy, "proportion") {
		return "proportion"
	}

	return ""
}

func queueStrategySources(strategy string) []string {
	switch strategy {
	case "capacity":
		return []string{"pkg/scheduler/plugins/capacity/capacity.go:AddJobEnqueueableFn"}
	case "proportion":
		return []string{"pkg/scheduler/plugins/proportion/proportion.go:AddJobEnqueueableFn"}
	default:
		return []string{"pkg/scheduler/plugins/proportion", "pkg/scheduler/plugins/capacity"}
	}
}

func queueStrategyNote(strategy string) string {
	switch queueBaseStrategy(strategy) {
	case "capacity":
		return "capacity uses the same enqueue quota formula; during allocate it limits tasks by realCapability-allocated and uses Queue.spec.deserved for ordering/share"
	case "proportion":
		return "proportion uses this formula for enqueue; during allocate it computes fair-share deserved and limits tasks by deserved-allocated"
	default:
		return "no proportion or capacity JobEnqueueable rule is configured"
	}
}

func podGroupSummaries(groups []cluster.PodGroup, target cluster.PodGroup) []PodGroupSummary {
	result := make([]PodGroupSummary, 0, len(groups))
	now := time.Now()

	for _, group := range groups {
		createdAt := ""
		ageSeconds := int64(0)

		if !group.CreationTimestamp.IsZero() {
			createdAt = group.CreationTimestamp.UTC().Format(time.RFC3339)
			ageSeconds = max(int64(now.Sub(group.CreationTimestamp).Seconds()), 0)
		}

		result = append(result, PodGroupSummary{
			Namespace:         group.Namespace,
			Name:              group.Name,
			Target:            group.Namespace == target.Namespace && group.Name == target.Name,
			Phase:             group.Phase,
			PriorityClassName: group.PriorityClassName,
			Priority:          copyInt32(group.Priority),
			CreatedAt:         createdAt,
			AgeSeconds:        ageSeconds,
			MinMember:         group.MinMember,
			Resources:         resourceListValues(group.MinResources),
		})
	}

	return result
}

func workloadRows(
	target *corev1.Pod,
	tasks []corev1.Pod,
	checks []Check,
) []WorkloadRow {
	if len(tasks) == 0 && target != nil {
		tasks = []corev1.Pod{*target.DeepCopy()}
	}

	result := make([]WorkloadRow, 0, len(tasks))

	for index := range tasks {
		pod := &tasks[index]
		containers := make([]string, 0, len(pod.Spec.Containers))

		for _, container := range pod.Spec.Containers {
			containers = append(containers, container.Name)
		}

		result = append(result, WorkloadRow{
			Namespace:  pod.Namespace,
			Pod:        pod.Name,
			Containers: containers,
			Resources:  podResourceValues(pod),
			Checks:     cloneChecks(checks),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Namespace+"/"+result[i].Pod < result[j].Namespace+"/"+result[j].Pod
	})

	return result
}

func pluginHookChecks(
	hooks []PluginHookReport,
	actions map[string]bool,
	wantedHooks map[string]bool,
	stage string,
) []Check {
	result := make([]Check, 0)
	seen := map[string]bool{}

	for _, hook := range hooks {
		if !actions[hook.Action] || !wantedHooks[hook.Hook] {
			continue
		}

		key := fmt.Sprintf("%d/%d/%s/%s", hook.Tier, hook.Order, hook.Plugin, hook.Hook)
		if seen[key] {
			continue
		}

		seen[key] = true
		id := fmt.Sprintf(
			"plugin.%s.t%d-p%d.%s",
			identifier(hook.Plugin),
			hook.Tier+1,
			hook.Order+1,
			identifier(hook.Hook),
		)
		name := fmt.Sprintf(
			"T%d.%d %s %s",
			hook.Tier+1,
			hook.Order+1,
			hook.Plugin,
			strings.TrimPrefix(hook.Hook, "Add"),
		)

		switch {
		case !hook.EnabledDeterminate:
			result = append(result, model.Unknown(
				id,
				stage,
				name,
				hook.Reason,
				nil,
				hook.Source,
			))
		case !hook.Enabled:
			result = append(result, model.Skipped(
				id,
				stage,
				name,
				hook.Reason,
				hook.Source,
			))
		case hook.Determinate:
			result = append(result, model.Known(
				id,
				stage,
				name,
				hook.Passed,
				hook.Reason,
				hook.Source,
			))
		default:
			result = append(result, model.Unknown(
				id,
				stage,
				name,
				hook.Reason,
				nil,
				hook.Source,
			))
		}
	}

	return result
}

func unmatchedPluginChecks(
	hooks []PluginHookReport,
	actions map[string]bool,
	stage string,
	wantedHook string,
) []Check {
	result := make([]Check, 0)
	seen := map[string]bool{}

	for _, hook := range hooks {
		if !actions[hook.Action] || hook.Hook != "source hook registration" {
			continue
		}

		key := fmt.Sprintf("%d/%d/%s", hook.Tier, hook.Order, hook.Plugin)
		if seen[key] {
			continue
		}

		seen[key] = true
		result = append(result, model.Unknown(
			fmt.Sprintf(
				"plugin.%s.t%d-p%d.source-registration",
				identifier(hook.Plugin),
				hook.Tier+1,
				hook.Order+1,
			),
			stage,
			fmt.Sprintf(
				"T%d.%d %s %s registration",
				hook.Tier+1,
				hook.Order+1,
				hook.Plugin,
				wantedHook,
			),
			hook.Reason+"; "+wantedHook+" cannot be evaluated until the selected branch matches the configured plugin",
			nil,
			hook.Source,
		))
	}

	return result
}

func hookInventoryVerified(report Report) bool {
	if !report.HooksInspected || !report.Policy.Determinate || !report.Policy.ActiveDeterminate {
		return false
	}

	knownActions := map[string]bool{
		"enqueue":  true,
		"allocate": true,
		"backfill": true,
		"preempt":  true,
		"reclaim":  true,
		"shuffle":  true,
	}

	for _, action := range report.Policy.Actions {
		if !knownActions[action] {
			return false
		}
	}

	return true
}

func hasCheck(checks []Check, id string) bool {
	for _, check := range checks {
		if check.ID == id {
			return true
		}
	}

	return false
}

func stateFromChecks(checks []Check) StageState {
	failures := make([]string, 0)
	unknown := 0
	executed := 0

	for _, check := range checks {
		if check.Skipped {
			continue
		}

		executed++

		if !check.Determinate {
			unknown++

			continue
		}

		if !check.Passed {
			failures = append(failures, check.Name)
		}
	}

	if len(failures) > 0 {
		return StageState{
			Outcome:    model.OutcomeFail,
			Conclusion: strings.Join(failures, "；"),
		}
	}

	if unknown > 0 || executed == 0 {
		return StageState{
			Outcome:    model.OutcomeUnknown,
			Conclusion: fmt.Sprintf("没有确定失败，仍有 %d 项需要运行时或源码证据", unknown),
		}
	}

	return StageState{
		Outcome:    model.OutcomePass,
		Conclusion: "所有已执行检查均通过",
	}
}

func diagnoseReport(report Report) Diagnosis {
	for _, check := range report.Checks {
		if check.Stage == "preflight" && check.Determinate && !check.Passed && !check.Skipped {
			return diagnosisForFailure(check, report.Enqueue.Queue.Name)
		}
	}

	if allSchedulingStagesSkipped(report) {
		for _, check := range report.Checks {
			if check.Stage != "preflight" || check.Determinate || check.Skipped {
				continue
			}

			return Diagnosis{
				RootCause: "前置证据不足，无法继续调度分析：" + check.Name + " — " + check.Reason,
				Suggestions: []string{
					"重试分析以重新读取 Kubernetes informer 缓存",
					"检查 Kubernetes API 连通性以及 informer 是否完成同步",
				},
			}
		}
	}

	for _, check := range report.Enqueue.Checks {
		if check.Determinate && !check.Passed && !check.Skipped {
			return diagnosisForFailure(check, report.Enqueue.Queue.Name)
		}
	}

	for _, row := range report.JobValid.Rows {
		for _, check := range row.Checks {
			if check.Determinate && !check.Passed && !check.Skipped {
				return diagnosisForFailure(check, report.Enqueue.Queue.Name)
			}
		}
	}

	failureCounts := map[string]int{}
	failureChecks := map[string]Check{}

	for _, node := range report.Allocate.Nodes {
		for _, check := range node.Checks {
			if check.Determinate && !check.Passed && !check.Skipped {
				failureCounts[check.ID]++
				failureChecks[check.ID] = check
			}
		}
	}

	for _, id := range orderedNodeCheckIDs(report.Allocate.Nodes) {
		count := failureCounts[id]
		if count == len(report.Allocate.Nodes) && count > 0 {
			return diagnosisForFailure(failureChecks[id], report.Enqueue.Queue.Name)
		}
	}

	if len(report.Allocate.Nodes) > 0 {
		allKnownFailed := true

		for _, node := range report.Allocate.Nodes {
			if node.Passed || !node.Determinate {
				allKnownFailed = false

				break
			}
		}

		if allKnownFailed {
			type countedFailure struct {
				name  string
				count int
			}

			counts := make([]countedFailure, 0, len(failureCounts))

			for id, count := range failureCounts {
				counts = append(counts, countedFailure{
					name:  failureChecks[id].Name,
					count: count,
				})
			}

			sort.Slice(counts, func(i, j int) bool {
				if counts[i].count != counts[j].count {
					return counts[i].count > counts[j].count
				}

				return counts[i].name < counts[j].name
			})

			suggestions := make([]string, 0, min(len(counts), 3)+1)

			for index, failure := range counts {
				if index == 3 {
					break
				}

				suggestions = append(suggestions, fmt.Sprintf(
					"处理 %s（影响 %d 个节点）",
					failure.name,
					failure.count,
				))
			}

			suggestions = append(suggestions, "按节点表中的红色叉号逐行确认可修复条件")

			return Diagnosis{
				RootCause:   "所有候选节点都被确定过滤，但各节点的首要失败项不同",
				Suggestions: suggestions,
			}
		}
	}

	if report.Enqueue.State.Outcome == model.OutcomeUnknown ||
		report.JobValid.State.Outcome == model.OutcomeUnknown ||
		report.Allocate.State.Outcome == model.OutcomeUnknown {
		return Diagnosis{
			RootCause: "现有规则和运行时证据不足以唯一确定 Pending 根因",
			Suggestions: []string{
				"查看页面中的灰色问号及其证据说明",
				"结合所选分支源码和 scheduler 日志继续分析插件私有状态",
			},
		}
	}

	return Diagnosis{
		RootCause: "常见 enqueue、JobValid 和节点过滤规则未发现确定失败",
		Suggestions: []string{
			"继续检查队列排序、公平性、抢占及绑定阶段的瞬时状态",
			"对照所选分支源码与 scheduler 日志确认实际执行路径",
		},
	}
}

func allSchedulingStagesSkipped(report Report) bool {
	return report.Enqueue.State.Outcome == model.OutcomeSkipped &&
		report.JobValid.State.Outcome == model.OutcomeSkipped &&
		report.Allocate.State.Outcome == model.OutcomeSkipped
}

func orderedNodeCheckIDs(nodes []NodeResult) []string {
	preferred := []string{
		"node.ready",
		"node.resources",
		"node.pod-count",
		"node.schedulable",
		"node.selector",
		"node.affinity",
		"node.taints",
		"node.ports",
		"node.pod-affinity",
		"node.volume-limits",
		"node.volume-zone",
		"node.topology-spread",
		"node.proportional",
	}
	result := make([]string, 0, len(preferred))
	seen := map[string]bool{}

	for _, id := range preferred {
		result = append(result, id)
		seen[id] = true
	}

	for _, node := range nodes {
		for _, check := range node.Checks {
			if check.ID == "node.pv-bind" || seen[check.ID] {
				continue
			}

			seen[check.ID] = true
			result = append(result, check.ID)
		}
	}

	return result
}

func diagnosisForFailure(check Check, queueName string) Diagnosis {
	switch check.ID {
	case "task.exists":
		return Diagnosis{
			RootCause:   "目标 Pod 不存在或已经被删除",
			Suggestions: []string{"刷新 Pending Pod 列表后重新选择目标"},
		}
	case "task.pending", "task.unbound":
		return Diagnosis{
			RootCause:   "目标 Pod 不属于未绑定的 Pending 调度问题",
			Suggestions: []string{"若 Pod 已绑定节点，请转查 kubelet、容器运行时、镜像或卷挂载"},
		}
	case "task.scheduler":
		return Diagnosis{
			RootCause:   "目标 Pod 没有可用的 Volcano scheduler 实例处理",
			Suggestions: []string{"核对 Pod spec.schedulerName 与 vc-scheduler 启动参数", "恢复 Volcano scheduler Pod Ready 状态"},
		}
	case "task.scheduling-gates":
		return Diagnosis{
			RootCause:   "Pod 仍带有 Kubernetes scheduling gate",
			Suggestions: []string{"由负责该 gate 的控制器移除 gate 后再分析 Volcano 调度"},
		}
	case "job.podgroup", "job.podgroup.exists":
		return Diagnosis{
			RootCause:   "Pod 缺少有效的 PodGroup 关联",
			Suggestions: []string{"检查 scheduling.k8s.io/group-name", "确认同命名空间 PodGroup 已创建且名称一致"},
		}
	case "job.queue.exists":
		return Diagnosis{
			RootCause:   "PodGroup 所属队列不存在",
			Suggestions: []string{"创建或修正队列 " + queueName, "确认 scheduler 的默认队列参数"},
		}
	case "queue.enqueue-capacity":
		return Diagnosis{
			RootCause:   "队列当前没有足够的可入队资源",
			Suggestions: []string{"释放该队列中已分配或已入队的资源", "等待运行中的 PodGroup 完成，或调整 Queue capability/guarantee"},
		}
	case "job.enqueue.evidence":
		if strings.Contains(strings.ToLower(check.Reason), "queue resource quota insufficient") {
			return Diagnosis{
				RootCause:   "队列当前没有足够的可入队资源",
				Suggestions: []string{"释放该队列中已分配或已入队的资源", "等待运行中的 PodGroup 完成，或调整 Queue capability/guarantee"},
			}
		}

		return Diagnosis{
			RootCause: "Volcano 入队阶段或 JobEnqueueable 插件拒绝了 PodGroup：" + check.Reason,
			Suggestions: []string{
				"根据拒绝原因定位对应的 JobEnqueueable 插件和配置",
				"结合 scheduler 日志与所选分支源码确认插件私有状态",
			},
		}
	case "node.resources", "allocate.nodes":
		return Diagnosis{
			RootCause:   "没有节点具备足够的 Volcano 可用资源",
			Suggestions: []string{"释放节点资源或扩容集群", "降低 Pod 请求，或等待 releasing 资源真正回收"},
		}
	case "node.taints":
		return Diagnosis{
			RootCause:   "所有候选节点都存在 Pod 未容忍的污点",
			Suggestions: []string{"为 Pod 增加正确的 toleration", "仅在符合运维策略时移除节点污点"},
		}
	case "node.selector", "node.affinity":
		return Diagnosis{
			RootCause:   "Pod 的节点选择或必需亲和性排除了所有候选节点",
			Suggestions: []string{"修正 nodeSelector/required nodeAffinity", "为符合条件的节点补充正确标签"},
		}
	}

	return Diagnosis{
		RootCause:   check.Name + " 未通过：" + check.Reason,
		Suggestions: []string{"先处理该确定失败项，再重新运行分析"},
	}
}

func checksForStage(checks []Check, stage string) []Check {
	result := make([]Check, 0)

	for _, check := range checks {
		if check.Stage == stage {
			result = append(result, check)
		}
	}

	return result
}

func knownStageFailure(checks []Check, stage string) (Check, bool) {
	for _, check := range checks {
		if check.Stage == stage && check.Determinate && !check.Passed && !check.Skipped {
			return check, true
		}
	}

	return Check{}, false
}

func skippedChecks(checks []Check, stage, reason string) []Check {
	result := make([]Check, 0, len(checks))

	for _, check := range checks {
		result = append(result, model.Skipped(
			check.ID,
			stage,
			check.Name,
			reason,
			check.Source,
		))
	}

	return result
}

func removeCheck(checks []Check, id string) []Check {
	result := make([]Check, 0, len(checks))

	for _, check := range checks {
		if check.ID != id {
			result = append(result, check)
		}
	}

	return result
}

func replaceCheck(checks []Check, id string, replacement Check) []Check {
	result := make([]Check, 0, len(checks)+1)
	replaced := false

	for _, check := range checks {
		if check.ID == id {
			if !replaced {
				result = append(result, replacement)
				replaced = true
			}

			continue
		}

		result = append(result, check)
	}

	if !replaced {
		result = append(result, replacement)
	}

	return result
}

func removeCheckPrefix(checks []Check, prefix string) []Check {
	result := make([]Check, 0, len(checks))

	for _, check := range checks {
		if !strings.HasPrefix(check.ID, prefix) {
			result = append(result, check)
		}
	}

	return result
}

func hasHookPlugin(hooks []PluginHookReport, hookName, pluginName string) bool {
	for _, hook := range hooks {
		if hook.Hook == hookName && strings.EqualFold(hook.Plugin, pluginName) {
			return true
		}
	}

	return false
}

func hasEnabledHookPlugin(hooks []PluginHookReport, hookName, pluginName string) bool {
	for _, hook := range hooks {
		if hook.Hook == hookName &&
			strings.EqualFold(hook.Plugin, pluginName) &&
			hook.EnabledDeterminate && hook.Enabled {
			return true
		}
	}

	return false
}

func cloneChecks(checks []Check) []Check {
	return append([]Check(nil), checks...)
}

func recomputeNodeOutcome(node *NodeResult) {
	node.Passed = true
	node.Determinate = true
	knownFailure := false
	unknown := false

	for _, check := range node.Checks {
		if check.Skipped {
			continue
		}

		node.Passed = node.Passed && check.Passed && check.Determinate
		knownFailure = knownFailure || check.Determinate && !check.Passed
		unknown = unknown || !check.Determinate
	}

	node.Determinate = knownFailure || !unknown
}

func identifier(value string) string {
	value = strings.ToLower(value)
	var result strings.Builder

	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' || character == '-' {
			result.WriteRune(character)
		} else {
			result.WriteByte('-')
		}
	}

	return strings.Trim(result.String(), "-")
}

func podResourceValues(pod *corev1.Pod) map[string]float64 {
	result := map[string]float64{string(corev1.ResourcePods): 1}

	for _, container := range pod.Spec.Containers {
		addResourceValues(result, resourceListValues(container.Resources.Requests))
	}

	maxInit := map[string]float64{}

	for _, container := range pod.Spec.InitContainers {
		values := resourceListValues(container.Resources.Requests)

		for name, value := range values {
			if value > maxInit[name] {
				maxInit[name] = value
			}
		}
	}

	addResourceValues(result, maxInit)
	addResourceValues(result, resourceListValues(pod.Spec.Overhead))

	return result
}

func resourceListValues(resources corev1.ResourceList) map[string]float64 {
	result := make(map[string]float64, len(resources))
	requestBacked := map[string]bool{}

	for name, quantity := range resources {
		originalName := string(name)
		resourceName := canonicalResourceName(originalName)
		fromRequest := strings.HasPrefix(originalName, "requests.")

		if requestBacked[resourceName] && !fromRequest {
			continue
		}

		if resourceName == string(corev1.ResourceCPU) {
			result[resourceName] = float64(quantity.MilliValue()) / 1000
			requestBacked[resourceName] = fromRequest || requestBacked[resourceName]

			continue
		}

		result[resourceName] = float64(quantity.Value())
		requestBacked[resourceName] = fromRequest || requestBacked[resourceName]
	}

	return result
}

func canonicalResourceName(name string) string {
	trimmed := strings.TrimSpace(name)
	if strings.HasPrefix(strings.ToLower(trimmed), "requests.") {
		return trimmed[len("requests."):]
	}

	return trimmed
}

func addResourceValues(destination, source map[string]float64) {
	for name, value := range source {
		destination[name] += value
	}
}

func copyFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}

	copy := *value

	return &copy
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}

	copy := *value

	return &copy
}
