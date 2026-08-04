package filter

import (
	"fmt"
	"sort"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
)

func evaluateNode(
	pod *corev1.Pod,
	node *corev1.Node,
	cacheNode cluster.CacheNode,
	cacheFound bool,
) model.NodeResult {
	result := model.NodeResult{
		Name:        node.Name,
		Passed:      true,
		Determinate: true,
	}

	selectorOK := true

	for key, value := range pod.Spec.NodeSelector {
		actual, found := node.Labels[key]
		selectorOK = selectorOK && found && actual == value
	}

	result.Checks = append(result.Checks, model.Known(
		"node.selector",
		"filter",
		"Node selector",
		selectorOK,
		fmt.Sprintf("wanted=%v", pod.Spec.NodeSelector),
		nil,
	))

	result.Checks = append(result.Checks, model.Known(
		"node.affinity",
		"filter",
		"Required node affinity",
		requiredNodeAffinityMatches(pod, node),
		"requiredDuringSchedulingIgnoredDuringExecution",
		[]string{"pkg/scheduler/plugins/predicates/predicates.go:NodeAffinity"},
	))

	result.Checks = append(result.Checks, model.Known(
		"node.schedulable",
		"filter",
		"Node schedulable",
		!node.Spec.Unschedulable,
		"spec.unschedulable",
		nil,
	))

	result.Checks = append(result.Checks, model.Known(
		"node.ready",
		"filter",
		"Node Ready",
		nodeReady(node),
		"Ready condition",
		nil,
	))

	result.Checks = append(result.Checks, model.Known(
		"node.taints",
		"filter",
		"Taints tolerated",
		taintsTolerated(node.Spec.Taints, pod.Spec.Tolerations),
		"NoSchedule/NoExecute taints",
		nil,
	))

	requests := podRequests(pod)

	if cacheFound {
		resourceOK, reason := fits(requests, cacheNode.Idle)
		resourceCheck := model.Known(
			"node.resources",
			"filter",
			"Requested resources fit Volcano idle",
			resourceOK,
			reason,
			[]string{"SIGUSR2 Node (...).idle"},
		)
		resourceCheck.Evidence = cacheNode

		result.Checks = append(result.Checks, resourceCheck)
	} else {
		allocatable := resourceListValues(node.Status.Allocatable)
		upperBoundOK, reason := fits(requests, allocatable)

		result.Checks = append(result.Checks, model.Unknown(
			"node.resources",
			"filter",
			"Requested resources fit Volcano idle",
			fmt.Sprintf(
				"cache entry missing; Kubernetes allocatable upper-bound fit=%t: %s",
				upperBoundOK,
				reason,
			),
			allocatable,
			[]string{"SIGUSR2 Node (...).idle"},
		))
	}

	knownFailure := false
	unknown := false

	for _, check := range result.Checks {
		result.Passed = result.Passed && check.Passed && check.Determinate
		knownFailure = knownFailure || (check.Determinate && !check.Passed)
		unknown = unknown || !check.Determinate
	}

	result.Determinate = knownFailure || !unknown

	return result
}

func eligibleReadyNodeNames(nodes []corev1.Node, pod *corev1.Pod) map[string]struct{} {
	result := map[string]struct{}{}

	for i := range nodes {
		node := &nodes[i]

		if !nodeReady(node) {
			continue
		}

		matched := true

		for key, value := range pod.Spec.NodeSelector {
			actual, found := node.Labels[key]

			if !found || actual != value {
				matched = false

				break
			}
		}

		matched = matched && requiredNodeAffinityMatches(pod, node)

		if matched {
			result[node.Name] = struct{}{}
		}
	}

	return result
}

func matchCacheNodes(
	cacheNodes map[string]cluster.CacheNode,
	expected map[string]struct{},
) (map[string]cluster.CacheNode, []string) {
	matched := make(map[string]cluster.CacheNode, len(expected))
	missing := make([]string, 0)

	for name := range expected {
		node, found := cacheNodes[name]
		if !found {
			missing = append(missing, name)

			continue
		}

		matched[name] = node
	}

	sort.Strings(missing)

	return matched, missing
}
