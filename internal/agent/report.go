package agent

import (
	"fmt"
	"strings"

	"github.com/volcano-sh/volens/internal/agent/model"
)

func finalizeReport(report *Report) {
	report.Actions = buildActionReports(*report)
	report.Passed = true

	for _, action := range report.Actions {
		for _, check := range action.Checks {
			report.Passed = report.Passed && check.Passed && check.Determinate
		}

		for _, plugin := range action.Plugins {
			report.Passed = report.Passed && plugin.Passed && plugin.Determinate
		}

		for _, node := range action.Nodes {
			report.Passed = report.Passed && node.Passed && node.Determinate
		}
	}

	failures := knownFailures(*report)
	if len(failures) > 0 {
		report.Conclusion = fmt.Sprintf(
			"Found %d deterministic failure(s):\n- %s",
			len(failures),
			strings.Join(failures, "\n- "),
		)

		return
	}

	if report.Passed {
		report.Conclusion = "All inspected action and plugin checks passed; inspect queue ordering, fairness, and scheduler runtime state."

		return
	}

	report.Conclusion = "Common deterministic rules found no failure; one or more configured action or branch-specific plugin checks require source and scheduler-log analysis."
}

func knownFailures(report Report) []string {
	failures := make([]string, 0)

	for _, check := range report.Checks {
		if check.Determinate && !check.Passed {
			failures = append(failures, check.Name+": "+check.Reason)
		}
	}

	for _, plugin := range report.PluginHooks {
		if plugin.Determinate && !plugin.Passed {
			failures = append(
				failures,
				fmt.Sprintf(
					"%s plugin %s %s: %s",
					plugin.Action,
					plugin.Plugin,
					plugin.Hook,
					plugin.Reason,
				),
			)
		}
	}

	return failures
}

func hasUnknown(report Report) bool {
	for _, check := range report.Checks {
		if !check.Determinate {
			return true
		}
	}

	for _, plugin := range report.PluginHooks {
		if !plugin.Determinate {
			return true
		}
	}

	for _, action := range report.Actions {
		for _, check := range action.Checks {
			if !check.Determinate {
				return true
			}
		}
	}

	return false
}

func buildActionReports(report Report) []ActionReport {
	checksByAction := map[string][]Check{}
	nodesByAction := splitNodesByAction(report.Nodes)
	pluginsByAction := map[string][]PluginHookReport{}
	observedOrder := make([]string, 0)
	observed := map[string]bool{}

	observe := func(stage string) {
		if stage == "" || observed[stage] {
			return
		}

		observed[stage] = true
		observedOrder = append(observedOrder, stage)
	}

	for _, check := range report.Checks {
		stage := normalizedActionStage(check.Stage)
		checksByAction[stage] = append(checksByAction[stage], check)
		observe(stage)
	}

	for action := range nodesByAction {
		observe(action)
	}

	for _, plugin := range report.PluginHooks {
		stage := normalizedActionStage(plugin.Action)
		pluginsByAction[stage] = append(pluginsByAction[stage], plugin)
		observe(stage)
	}

	actions := make([]ActionReport, 0, len(report.Policy.Actions)+len(observedOrder))
	firstIndex := map[string]int{}

	appendAction := func(name string, configured bool, order int) {
		action := ActionReport{
			Name:       name,
			Order:      order,
			Configured: configured,
		}

		_, found := firstIndex[name]
		if !found {
			firstIndex[name] = len(actions)
		}

		if configured || !found {
			action.Checks = append(action.Checks, checksByAction[name]...)
			action.Plugins = append(action.Plugins, pluginsByAction[name]...)
			action.Nodes = append(action.Nodes, nodesByAction[name]...)
		}

		actions = append(actions, action)
	}

	if observed["preflight"] {
		appendAction("preflight", false, -1)
	}

	for order, action := range report.Policy.Actions {
		appendAction(action, true, order)
	}

	for _, action := range observedOrder {
		if action == "preflight" {
			continue
		}

		if _, found := firstIndex[action]; found {
			continue
		}

		appendAction(action, false, len(actions))
	}

	for index := range actions {
		if !actions[index].Configured || nativeActionCoverage(actions[index].Name) {
			continue
		}

		actions[index].Checks = append(actions[index].Checks, model.Unknown(
			"action."+actions[index].Name+".coverage",
			actions[index].Name,
			"Configured action fully reproduced",
			"the action is present in the observed scheduler configuration, but no deterministic local rule module reproduces its complete target-job control flow",
			nil,
			[]string{"pkg/scheduler/actions/" + actions[index].Name},
		))
	}

	return actions
}

func nativeActionCoverage(action string) bool {
	return action == "enqueue" || action == "allocate"
}

func normalizedActionStage(stage string) string {
	switch stage {
	case "validate":
		return "preflight"
	case "filter", "evidence", "jobValid":
		return "allocate"
	default:
		return stage
	}
}

func splitNodesByAction(nodes []NodeResult) map[string][]NodeResult {
	result := map[string][]NodeResult{}

	for _, node := range nodes {
		nodesByAction := map[string]NodeResult{}
		knownFailureByAction := map[string]bool{}
		unknownByAction := map[string]bool{}
		nodeOrder := make([]string, 0)

		for _, check := range node.Checks {
			actionName := normalizedActionStage(check.Stage)
			actionNode, found := nodesByAction[actionName]
			if !found {
				actionNode = NodeResult{
					Name:        node.Name,
					Passed:      true,
					Determinate: true,
				}
				nodeOrder = append(nodeOrder, actionName)
			}

			actionNode.Checks = append(actionNode.Checks, check)
			actionNode.Passed = actionNode.Passed && check.Passed && check.Determinate
			knownFailureByAction[actionName] = knownFailureByAction[actionName] ||
				(check.Determinate && !check.Passed)
			unknownByAction[actionName] = unknownByAction[actionName] || !check.Determinate
			nodesByAction[actionName] = actionNode
		}

		for _, actionName := range nodeOrder {
			actionNode := nodesByAction[actionName]
			actionNode.Determinate = knownFailureByAction[actionName] || !unknownByAction[actionName]
			result[actionName] = append(result[actionName], actionNode)
		}
	}

	return result
}
