package filter

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

func requiredNodeAffinityMatches(pod *corev1.Pod, node *corev1.Node) bool {
	affinity := pod.Spec.Affinity

	if affinity == nil || affinity.NodeAffinity == nil ||
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}

	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms

	for _, term := range terms {
		if nodeSelectorTermMatches(term, node) {
			return true
		}
	}

	return false
}

func nodeSelectorTermMatches(term corev1.NodeSelectorTerm, node *corev1.Node) bool {
	if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
		return false
	}

	for _, requirement := range term.MatchExpressions {
		value, found := node.Labels[requirement.Key]

		if !nodeSelectorRequirementMatches(requirement, value, found) {
			return false
		}
	}

	for _, requirement := range term.MatchFields {
		value, found := nodeField(node, requirement.Key)

		if !nodeSelectorRequirementMatches(requirement, value, found) {
			return false
		}
	}

	return true
}

func nodeField(node *corev1.Node, key string) (string, bool) {
	if key == "metadata.name" {
		return node.Name, true
	}

	return "", false
}

func nodeSelectorRequirementMatches(
	requirement corev1.NodeSelectorRequirement,
	value string,
	found bool,
) bool {
	switch requirement.Operator {
	case corev1.NodeSelectorOpIn:
		return found && stringIn(value, requirement.Values)
	case corev1.NodeSelectorOpNotIn:
		return !found || !stringIn(value, requirement.Values)
	case corev1.NodeSelectorOpExists:
		return found
	case corev1.NodeSelectorOpDoesNotExist:
		return !found
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		if !found || len(requirement.Values) != 1 {
			return false
		}

		actual, actualErr := strconv.ParseInt(value, 10, 64)
		expected, expectedErr := strconv.ParseInt(requirement.Values[0], 10, 64)
		if actualErr != nil || expectedErr != nil {
			return false
		}

		if requirement.Operator == corev1.NodeSelectorOpGt {
			return actual > expected
		}

		return actual < expected
	default:
		return false
	}
}

func stringIn(value string, candidates []string) bool {
	for _, candidate := range candidates {
		if value == candidate {
			return true
		}
	}

	return false
}
