package filter

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func nodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

func taintsTolerated(taints []corev1.Taint, tolerations []corev1.Toleration) bool {
	for i := range taints {
		taint := &taints[i]

		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}

		tolerated := false

		for j := range tolerations {
			if tolerations[j].ToleratesTaint(taint) {
				tolerated = true

				break
			}
		}

		if !tolerated {
			return false
		}
	}

	return true
}

// podRequests follows Kubernetes scheduling semantics, including restartable
// init containers, the max init-container request, and Pod overhead.
func podRequests(pod *corev1.Pod) map[string]float64 {
	requests := map[string]float64{
		string(corev1.ResourcePods): 1,
	}

	for i := range pod.Spec.Containers {
		addResources(requests, resourceListValues(pod.Spec.Containers[i].Resources.Requests))
	}

	restartableInit := map[string]float64{}
	maxInit := map[string]float64{}

	for i := range pod.Spec.InitContainers {
		container := &pod.Spec.InitContainers[i]
		current := resourceListValues(container.Resources.Requests)

		if container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			addResources(requests, current)
			addResources(restartableInit, current)
			current = cloneResources(restartableInit)
		} else {
			addResources(current, restartableInit)
		}

		maxResources(maxInit, current)
	}

	maxResources(requests, maxInit)
	addResources(requests, resourceListValues(pod.Spec.Overhead))

	return requests
}

func hasRestartableInitContainer(pod *corev1.Pod) bool {
	for index := range pod.Spec.InitContainers {
		restartPolicy := pod.Spec.InitContainers[index].RestartPolicy
		if restartPolicy != nil && *restartPolicy == corev1.ContainerRestartPolicyAlways {
			return true
		}
	}

	return false
}

func resourceListValues(resources corev1.ResourceList) map[string]float64 {
	result := make(map[string]float64, len(resources))

	for name, quantity := range resources {
		result[string(name)] = resourceValue(name, quantity)
	}

	return result
}

func resourceValue(name corev1.ResourceName, quantity resource.Quantity) float64 {
	if name == corev1.ResourceCPU {
		return float64(quantity.MilliValue()) / 1000
	}

	return float64(quantity.Value())
}

func addResources(destination, source map[string]float64) {
	for name, value := range source {
		destination[name] += value
	}
}

func maxResources(destination, candidate map[string]float64) {
	for name, value := range candidate {
		if value > destination[name] {
			destination[name] = value
		}
	}
}

func cloneResources(source map[string]float64) map[string]float64 {
	result := make(map[string]float64, len(source))

	for name, value := range source {
		result[name] = value
	}

	return result
}

func fits(request, available map[string]float64) (bool, string) {
	for name, value := range request {
		if value > available[name] {
			return false, fmt.Sprintf("%s request %.3f > available %.3f", name, value, available[name])
		}
	}

	return true, fmt.Sprintf("request=%v available=%v", request, available)
}
