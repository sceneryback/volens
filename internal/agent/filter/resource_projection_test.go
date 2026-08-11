package filter

import (
	"testing"

	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestResourceProjectionKeepsIdleFitUnknownWithoutPipelined(t *testing.T) {
	pod := testPendingPod()
	node := testReadyNode()
	cacheNode := cluster.CacheNode{
		Name: "node-a",
		Allocatable: map[string]float64{
			"cpu":  16,
			"pods": 100,
		},
		Idle: map[string]float64{
			"cpu":  2,
			"pods": 90,
		},
		Releasing: map[string]float64{
			"cpu": 1,
		},
	}

	result := evaluateNode(pod, &node, cacheNode, true)
	resourceCheck := findNodeCheck(t, result, "node.resources")

	if resourceCheck.Determinate || resourceCheck.Passed {
		t.Fatalf("resource check must remain unknown without Pipelined: %+v", resourceCheck)
	}
}

func TestResourceProjectionRejectsInsufficientIdleAndReleasingUpperBound(t *testing.T) {
	pod := testPendingPod()
	pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("6")
	node := testReadyNode()
	cacheNode := cluster.CacheNode{
		Name: "node-a",
		Allocatable: map[string]float64{
			"cpu":  16,
			"pods": 100,
		},
		Idle: map[string]float64{
			"cpu":  2,
			"pods": 90,
		},
		Releasing: map[string]float64{
			"cpu": 3,
		},
	}

	result := evaluateNode(pod, &node, cacheNode, true)
	resourceCheck := findNodeCheck(t, result, "node.resources")

	if !resourceCheck.Determinate || resourceCheck.Passed {
		t.Fatalf("idle+releasing upper-bound failure must be determinate: %+v", resourceCheck)
	}

	if !result.Resources["cpu"].Insufficient {
		t.Fatalf("failed CPU dimension must be marked insufficient: %+v", result.Resources["cpu"])
	}
}

func TestResourceProjectionRejectsNodeMissingPodsCapacity(t *testing.T) {
	pod := testPendingPod()
	node := testReadyNode()
	delete(node.Status.Allocatable, corev1.ResourcePods)

	cacheNode := cluster.CacheNode{
		Name: "node-a",
		Allocatable: map[string]float64{
			"cpu": 16,
		},
		Idle: map[string]float64{
			"cpu": 8,
		},
	}

	result := evaluateNode(pod, &node, cacheNode, true)
	podCount := findNodeCheck(t, result, "node.pod-count")
	resourceCheck := findNodeCheck(t, result, "node.resources")

	if !podCount.Determinate || podCount.Passed {
		t.Fatalf("missing pods capacity must fail pod-count check: %+v", podCount)
	}

	if !resourceCheck.Determinate || resourceCheck.Passed {
		t.Fatalf("missing pods capacity must fail resource check: %+v", resourceCheck)
	}

	if !result.Resources["pods"].Insufficient {
		t.Fatalf("missing pods dimension must be marked insufficient: %+v", result.Resources["pods"])
	}
}

func TestResourceProjectionKeepsKubernetesOnlyDeviceUnknown(t *testing.T) {
	const device = corev1.ResourceName("vendor.example/device")

	pod := testPendingPod()
	pod.Spec.Containers[0].Resources.Requests[device] = resource.MustParse("1")
	node := testReadyNode()
	node.Status.Allocatable[device] = resource.MustParse("8")
	cacheNode := cluster.CacheNode{
		Name: "node-a",
		Allocatable: map[string]float64{
			"cpu":  16,
			"pods": 100,
		},
		Idle: map[string]float64{
			"cpu":  2,
			"pods": 90,
		},
	}

	result := evaluateNode(pod, &node, cacheNode, true)
	resourceCheck := findNodeCheck(t, result, "node.resources")
	pair := result.Resources[string(device)]

	if pair.Total == nil || *pair.Total != 8 {
		t.Fatalf("Kubernetes device total was not preserved: %+v", pair)
	}

	if pair.Free != nil {
		t.Fatalf("missing cache device free value must remain unknown: %+v", pair)
	}

	if resourceCheck.Determinate || resourceCheck.Passed {
		t.Fatalf("cache-omitted device request must remain unknown: %+v", resourceCheck)
	}
}

func TestResourceProjectionRejectsDeviceMissingFromCacheAndKubernetes(t *testing.T) {
	const device = corev1.ResourceName("vendor.example/device")

	pod := testPendingPod()
	pod.Spec.Containers[0].Resources.Requests[device] = resource.MustParse("1")
	node := testReadyNode()
	cacheNode := cluster.CacheNode{
		Name: "node-a",
		Allocatable: map[string]float64{
			"cpu":  16,
			"pods": 100,
		},
		Idle: map[string]float64{
			"cpu":  2,
			"pods": 90,
		},
	}

	result := evaluateNode(pod, &node, cacheNode, true)
	resourceCheck := findNodeCheck(t, result, "node.resources")

	if !resourceCheck.Determinate || resourceCheck.Passed {
		t.Fatalf("device absent from cache and Kubernetes must be a deterministic failure: %+v", resourceCheck)
	}

	if result.Passed || !result.Determinate {
		t.Fatalf("node outcome must be deterministic failure when requested device is absent: %+v", result)
	}
}

func TestResourceProjectionSkipsFutureIdleForBestEffortBackfillTask(t *testing.T) {
	pod := testPendingPod()
	pod.Spec.Containers[0].Resources.Requests = nil
	node := testReadyNode()
	cacheNode := cluster.CacheNode{
		Name: "node-a",
		Allocatable: map[string]float64{
			"cpu":  16,
			"pods": 100,
		},
		Idle: map[string]float64{
			"cpu":  0,
			"pods": 0,
		},
	}

	result := evaluateNode(pod, &node, cacheNode, true)
	resourceCheck := findNodeCheck(t, result, "node.resources")

	if !resourceCheck.Skipped || !resourceCheck.Passed || !resourceCheck.Determinate {
		t.Fatalf("BestEffort backfill must not inherit allocate FutureIdle: %+v", resourceCheck)
	}

	if !volcanoBestEffort(podRequests(pod)) {
		t.Fatalf("pod request was not recognized as Volcano BestEffort: %v", podRequests(pod))
	}
}
