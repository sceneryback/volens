package filter

import (
	"fmt"
	"sort"
	"strings"

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
		Resources:   nodeResourcePairs(node, cacheNode, cacheFound),
	}

	for name := range podRequests(pod) {
		if _, found := result.Resources[name]; !found {
			result.Resources[name] = model.ResourcePair{}
		}
	}

	selectorOK := true

	for key, value := range pod.Spec.NodeSelector {
		actual, found := node.Labels[key]
		selectorOK = selectorOK && found && actual == value
	}

	result.Checks = append(result.Checks, model.Known(
		"node.selector",
		"allocate",
		"Node selector",
		selectorOK,
		fmt.Sprintf("wanted=%v", pod.Spec.NodeSelector),
		nil,
	))

	result.Checks = append(result.Checks, model.Known(
		"node.affinity",
		"allocate",
		"Required node affinity",
		requiredNodeAffinityMatches(pod, node),
		"requiredDuringSchedulingIgnoredDuringExecution",
		[]string{"pkg/scheduler/plugins/predicates/predicates.go:NodeAffinity"},
	))

	result.Checks = append(result.Checks, model.Known(
		"node.schedulable",
		"allocate",
		"Node schedulable",
		!node.Spec.Unschedulable,
		"spec.unschedulable",
		nil,
	))

	result.Checks = append(result.Checks, model.Known(
		"node.ready",
		"allocate",
		"Node Ready",
		nodeReady(node),
		"Ready condition",
		nil,
	))

	result.Checks = append(result.Checks, model.Known(
		"node.taints",
		"allocate",
		"Taints tolerated",
		taintsTolerated(node.Spec.Taints, pod.Spec.Tolerations),
		"NoSchedule/NoExecute taints",
		nil,
	))

	result.Checks = append(result.Checks, podCountCheck(cacheNode, cacheFound, node.Status.Allocatable))
	result.Checks = append(result.Checks, hostPortsCheck(pod))
	result.Checks = append(result.Checks, podAffinityCheck(pod))
	result.Checks = append(result.Checks, volumeLimitsCheck(pod))
	result.Checks = append(result.Checks, volumeZoneCheck(pod))
	result.Checks = append(result.Checks, topologySpreadCheck(pod))
	result.Checks = append(result.Checks, model.Skipped(
		"node.proportional",
		"allocate",
		"Proportional resource predicate",
		"the standard predicates plugin default disables this optional rule; a selected branch that enables it is represented by the dynamic plugin hook column",
		[]string{"pkg/scheduler/plugins/predicates/predicates.go:ProportionalPredicate"},
	))

	requests := podRequests(pod)

	if volcanoBestEffort(requests) {
		resourceCheck := model.Skipped(
			"node.resources",
			"allocate",
			"Requested resources fit Volcano FutureIdle",
			"Volcano allocate skips tasks whose Resreq is empty (the pods scalar is ignored); BestEffort tasks are handled by backfill without the allocate FutureIdle resource gate",
			[]string{
				"pkg/scheduler/actions/allocate/allocate.go",
				"pkg/scheduler/actions/backfill/backfill.go",
				"pkg/scheduler/api/resource_info.go:IsEmpty",
			},
		)
		resourceCheck.Evidence = requests
		result.Checks = append(result.Checks, resourceCheck)
	} else if cacheFound {
		resourceCheck, insufficientResources := resourceFitCheck(
			requests,
			cacheNode,
			node.Status.Allocatable,
		)
		if hasRestartableInitContainer(pod) {
			resourceCheck = model.Unknown(
				"node.resources",
				"allocate",
				"Requested resources fit Volcano FutureIdle",
				"the Pod has a restartable init container, and Volcano resource-request semantics vary by selected branch; this generic projection is not used as a definitive result",
				map[string]any{
					"requestProjection": requests,
					"cacheNode":         cacheNode,
				},
				[]string{"pkg/scheduler/api/pod_info.go:GetPodResourceRequest"},
			)
		}

		if resourceCheck.Determinate && !resourceCheck.Passed {
			for _, name := range insufficientResources {
				pair := result.Resources[name]
				pair.Insufficient = true
				result.Resources[name] = pair
			}
		}

		if resourceCheck.Evidence == nil {
			resourceCheck.Evidence = cacheNode
		}

		result.Checks = append(result.Checks, resourceCheck)
	} else {
		allocatable := resourceListValues(node.Status.Allocatable)
		upperBoundOK, reason := fits(requests, allocatable)

		result.Checks = append(result.Checks, model.Unknown(
			"node.resources",
			"allocate",
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

	result.Checks = append(result.Checks, model.Skipped(
		"node.pv-bind",
		"allocate",
		"PV binding after node selection",
		"Volcano performs GetPodVolumes/AllocateVolumes only after choosing a node; this diagnostic table does not claim that post-filter bind step ran",
		[]string{"pkg/scheduler/framework/statement.go:Allocate"},
	))

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

func volcanoBestEffort(requests map[string]float64) bool {
	for name, request := range requests {
		if name != string(corev1.ResourcePods) && request > 0 {
			return false
		}
	}

	return true
}

func nodeResourcePairs(
	node *corev1.Node,
	cacheNode cluster.CacheNode,
	cacheFound bool,
) map[string]model.ResourcePair {
	result := map[string]model.ResourcePair{}

	for name, total := range resourceListValues(node.Status.Allocatable) {
		value := total
		pair := result[name]
		pair.Total = &value
		result[name] = pair
	}

	if !cacheFound {
		return result
	}

	for name, total := range cacheNode.Allocatable {
		value := total
		pair := result[name]
		pair.Total = &value
		result[name] = pair
	}

	for name, free := range cacheNode.Idle {
		value := free
		pair := result[name]
		pair.Free = &value
		result[name] = pair
	}

	for name, used := range cacheNode.Used {
		value := used
		pair := result[name]
		pair.Used = &value
		result[name] = pair
	}

	for name, releasing := range cacheNode.Releasing {
		value := releasing
		pair := result[name]
		pair.Releasing = &value
		result[name] = pair
	}

	return result
}

func podCountCheck(
	cacheNode cluster.CacheNode,
	cacheFound bool,
	kubernetesAllocatable corev1.ResourceList,
) model.Check {
	if cacheFound {
		if free, found := cacheNode.Idle[string(corev1.ResourcePods)]; found {
			return model.Known(
				"node.pod-count",
				"allocate",
				"Pod count limit",
				free >= 1,
				fmt.Sprintf(
					"cache dump reports idle pods=%.0f; the target task contributes one implicit pods request",
					free,
				),
				[]string{"pkg/scheduler/plugins/predicates/predicates.go:NodePodNumberExceeded"},
			)
		}
	}

	if kubernetesPodsAllocatableOmitted(kubernetesAllocatable) {
		check := model.Known(
			"node.pod-count",
			"allocate",
			"Pod count limit",
			false,
			"node status.allocatable omits pods or sets it to zero; the target task has an implicit pods=1 request, so the node cannot satisfy Volcano's pod-count/resource dimension",
			[]string{"pkg/scheduler/plugins/predicates/predicates.go:NodePodNumberExceeded"},
		)
		check.Evidence = map[string]any{
			"k8sAllocatable": resourceListValues(kubernetesAllocatable),
			"cacheNode":      cacheNode,
		}

		return check
	}

	return model.Unknown(
		"node.pod-count",
		"allocate",
		"Pod count limit",
		"the cache dump does not contain the pods scalar",
		cacheNode,
		[]string{"pkg/scheduler/plugins/predicates/predicates.go:NodePodNumberExceeded"},
	)
}

func hostPortsCheck(pod *corev1.Pod) model.Check {
	if !podUsesHostPorts(pod) {
		return model.Known(
			"node.ports",
			"allocate",
			"Host ports",
			true,
			"the Pod requests no host ports",
			[]string{"pkg/scheduler/plugins/predicates/predicates.go:NodePorts"},
		)
	}

	return model.Unknown(
		"node.ports",
		"allocate",
		"Host ports",
		"the Pod requests host ports; occupied ports from the exact predicates NodeInfo snapshot are not present in the cache dump",
		nil,
		[]string{"pkg/scheduler/plugins/predicates/predicates.go:NodePorts"},
	)
}

func podAffinityCheck(pod *corev1.Pod) model.Check {
	if !hasRequiredPodAffinity(pod) {
		return model.Known(
			"node.pod-affinity",
			"allocate",
			"Inter-pod affinity",
			true,
			"the Pod has no required pod affinity or anti-affinity terms",
			[]string{"pkg/scheduler/plugins/predicates/predicates.go:InterPodAffinity"},
		)
	}

	return model.Unknown(
		"node.pod-affinity",
		"allocate",
		"Inter-pod affinity",
		"required pod affinity is present; exact evaluation needs the predicates PodLister and namespace snapshot from the scheduler Session",
		nil,
		[]string{"pkg/scheduler/plugins/predicates/predicates.go:InterPodAffinity"},
	)
}

func volumeLimitsCheck(pod *corev1.Pod) model.Check {
	if !podUsesVolumesWithNodeLimits(pod) {
		return model.Known(
			"node.volume-limits",
			"allocate",
			"CSI volume limits",
			true,
			"the Pod has no PVC, CSI, or ephemeral volume requiring node attachment limits",
			[]string{"pkg/scheduler/plugins/predicates/predicates.go:NodeVolumeLimits"},
		)
	}

	return model.Unknown(
		"node.volume-limits",
		"allocate",
		"CSI volume limits",
		"volume attachment limits require the scheduler's CSINode and attached-volume snapshot",
		nil,
		[]string{"pkg/scheduler/plugins/predicates/predicates.go:NodeVolumeLimits"},
	)
}

func volumeZoneCheck(pod *corev1.Pod) model.Check {
	if !podUsesPersistentVolumes(pod) {
		return model.Known(
			"node.volume-zone",
			"allocate",
			"PV and volume-zone affinity",
			true,
			"the Pod has no PVC-backed volumes",
			[]string{"pkg/scheduler/plugins/predicates/predicates.go:VolumeZone"},
		)
	}

	return model.Unknown(
		"node.volume-zone",
		"allocate",
		"PV and volume-zone affinity",
		"PVC/PV topology must be read from the matching scheduler snapshot; it is not emitted by the generic node cache dump",
		nil,
		[]string{"pkg/scheduler/plugins/predicates/predicates.go:VolumeZone"},
	)
}

func topologySpreadCheck(pod *corev1.Pod) model.Check {
	for _, constraint := range pod.Spec.TopologySpreadConstraints {
		if constraint.WhenUnsatisfiable == corev1.DoNotSchedule {
			return model.Unknown(
				"node.topology-spread",
				"allocate",
				"Pod topology spread",
				"a hard topology spread constraint is present; exact domain counts require the predicates PodLister snapshot",
				constraint,
				[]string{"pkg/scheduler/plugins/predicates/predicates.go:PodTopologySpread"},
			)
		}
	}

	return model.Known(
		"node.topology-spread",
		"allocate",
		"Pod topology spread",
		true,
		"the Pod has no DoNotSchedule topology spread constraint",
		[]string{"pkg/scheduler/plugins/predicates/predicates.go:PodTopologySpread"},
	)
}

func podUsesHostPorts(pod *corev1.Pod) bool {
	containers := append([]corev1.Container(nil), pod.Spec.InitContainers...)
	containers = append(containers, pod.Spec.Containers...)

	for _, container := range containers {
		for _, port := range container.Ports {
			if port.HostPort != 0 {
				return true
			}
		}
	}

	return false
}

func hasRequiredPodAffinity(pod *corev1.Pod) bool {
	if pod.Spec.Affinity == nil {
		return false
	}

	affinity := pod.Spec.Affinity

	return affinity.PodAffinity != nil &&
		len(affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0 ||
		affinity.PodAntiAffinity != nil &&
			len(affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0
}

func podUsesVolumesWithNodeLimits(pod *corev1.Pod) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil || volume.CSI != nil || volume.Ephemeral != nil {
			return true
		}
	}

	return false
}

func podUsesPersistentVolumes(pod *corev1.Pod) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil || volume.Ephemeral != nil {
			return true
		}
	}

	return false
}

func resourceFitCheck(
	requests map[string]float64,
	node cluster.CacheNode,
	kubernetesAllocatable corev1.ResourceList,
) (model.Check, []string) {
	upperBound := cloneResources(node.Idle)
	addResources(upperBound, node.Releasing)

	missing := make([]string, 0)
	insufficient := make([]string, 0)
	insufficientResources := make([]string, 0)

	for name, request := range requests {
		if request <= 0 {
			continue
		}

		_, idleFound := node.Idle[name]
		_, totalFound := node.Allocatable[name]

		if name == string(corev1.ResourcePods) &&
			kubernetesPodsAllocatableOmitted(kubernetesAllocatable) &&
			(!idleFound || node.Idle[name] == 0) &&
			(!totalFound || node.Allocatable[name] == 0) {
			insufficient = append(insufficient, fmt.Sprintf(
				"%s request %.3f > futureIdle %.3f because node status.allocatable omits pods",
				name,
				request,
				0.0,
			))
			insufficientResources = append(insufficientResources, name)

			continue
		}

		if !idleFound || !totalFound {
			if quantity, found := kubernetesAllocatable[corev1.ResourceName(name)]; found && !quantity.IsZero() {
				missing = append(missing, name+" (present only in Kubernetes allocatable)")
			} else {
				insufficient = append(insufficient, fmt.Sprintf(
					"%s request %.3f > futureIdle %.3f because the resource is absent from both Volcano cache and Kubernetes allocatable",
					name,
					request,
					0.0,
				))
				insufficientResources = append(insufficientResources, name)
			}

			continue
		}

		if request > upperBound[name] {
			insufficient = append(insufficient, fmt.Sprintf(
				"%s request %.3f > idle+releasing %.3f",
				name,
				request,
				upperBound[name],
			))
			insufficientResources = append(insufficientResources, name)
		}
	}

	sort.Strings(missing)
	sort.Strings(insufficient)

	if len(insufficient) > 0 {
		check := model.Known(
			"node.resources",
			"allocate",
			"Requested resources fit Volcano FutureIdle upper bound",
			false,
			strings.Join(insufficient, "; ")+"; even the idle+releasing upper bound is insufficient",
			[]string{"pkg/scheduler/api/node_info.go:FutureIdle", "SIGUSR2 Node (...).idle/releasing"},
		)
		check.Evidence = map[string]any{
			"request":               requests,
			"idle":                  node.Idle,
			"releasing":             node.Releasing,
			"upperBound":            upperBound,
			"insufficientResources": insufficientResources,
		}

		return check, insufficientResources
	}

	if len(missing) > 0 {
		return model.Unknown(
			"node.resources",
			"allocate",
			"Requested resources fit Volcano FutureIdle",
			"cache dump omits requested resource dimensions: "+strings.Join(missing, ", ")+"; plugin-private device state may own those resources",
			map[string]any{
				"request":        requests,
				"idle":           node.Idle,
				"releasing":      node.Releasing,
				"upperBound":     upperBound,
				"k8sAllocatable": resourceListValues(kubernetesAllocatable),
			},
			[]string{"pkg/scheduler/api/node_info.go:FutureIdle", "SIGUSR2 Node (...).idle/releasing"},
		), nil
	}

	return model.Unknown(
		"node.resources",
		"allocate",
		"Requested resources fit Volcano FutureIdle",
		"idle+releasing can fit, but the dump omits Pipelined from FutureIdle=Idle+Releasing-Pipelined, so a pass cannot be proven",
		map[string]any{
			"request":    requests,
			"idle":       node.Idle,
			"releasing":  node.Releasing,
			"upperBound": upperBound,
		},
		[]string{"pkg/scheduler/api/node_info.go:FutureIdle", "SIGUSR2 Node (...).idle/releasing"},
	), nil
}

func kubernetesPodsAllocatableOmitted(allocatable corev1.ResourceList) bool {
	quantity, found := allocatable[corev1.ResourcePods]

	return !found || quantity.IsZero()
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
