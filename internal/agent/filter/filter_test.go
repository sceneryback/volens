package filter

import (
	"errors"
	"strings"
	"testing"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEvaluateNodeUsesVolcanoIdleInsteadOfNodeAllocatable(t *testing.T) {
	pod := testPendingPod()
	node := testReadyNode()
	cacheNode := cluster.CacheNode{
		Name:        "node-a",
		Idle:        map[string]float64{"cpu": .5, "pods": 10},
		Allocatable: map[string]float64{"cpu": 16},
	}

	result := evaluateNode(pod, &node, cacheNode, true)
	resourceCheck := findNodeCheck(t, result, "node.resources")

	if resourceCheck.Passed {
		t.Fatal("request incorrectly fit Volcano idle")
	}

	if !resourceCheck.Determinate {
		t.Fatal("cache-backed result must be determinate")
	}

	if !strings.Contains(resourceCheck.Reason, "request 1.000 > idle+releasing 0.500") {
		t.Fatalf("reason=%q", resourceCheck.Reason)
	}
}

func TestEvaluateNodeMarksMissingCacheIndeterminate(t *testing.T) {
	pod := testPendingPod()
	node := testReadyNode()

	result := evaluateNode(pod, &node, cluster.CacheNode{}, false)
	resourceCheck := findNodeCheck(t, result, "node.resources")

	if result.Determinate {
		t.Fatal("node must be indeterminate without Volcano idle evidence")
	}

	if resourceCheck.Passed || resourceCheck.Determinate {
		t.Fatalf("resource check=%+v", resourceCheck)
	}

	if !strings.Contains(resourceCheck.Reason, "cache entry missing") {
		t.Fatalf("reason=%q", resourceCheck.Reason)
	}
}

func TestNodeSelectorRequiresLabelPresenceForEmptyValue(t *testing.T) {
	pod := testPendingPod()
	pod.Spec.NodeSelector = map[string]string{"dedicated": ""}
	node := testReadyNode()
	cacheNode := cluster.CacheNode{
		Name: "node-a",
		Idle: map[string]float64{
			"cpu":  2,
			"pods": 10,
		},
	}

	result := evaluateNode(pod, &node, cacheNode, true)

	if result.Checks[0].Passed {
		t.Fatalf("selector check=%+v", result.Checks[0])
	}

	eligible := eligibleReadyNodeNames([]corev1.Node{node}, pod)
	if _, found := eligible[node.Name]; found {
		t.Fatalf("node without the selected label was treated as eligible: %v", eligible)
	}
}

func TestMatchCacheNodesReportsMissingEligibleNodes(t *testing.T) {
	cacheNodes := map[string]cluster.CacheNode{
		"node-a": {
			Name: "node-a",
		},
	}
	expected := map[string]struct{}{
		"node-a": {},
		"node-b": {},
	}

	matched, missing := matchCacheNodes(cacheNodes, expected)

	if len(matched) != 1 || matched["node-a"].Name != "node-a" {
		t.Fatalf("matched=%v", matched)
	}

	if len(missing) != 1 || missing[0] != "node-b" {
		t.Fatalf("missing=%v", missing)
	}
}

func TestEvaluateIncludesCommonAndPluginResults(t *testing.T) {
	pod := testPendingPod()
	node := testReadyNode()
	dump := cluster.CacheDump{
		Scheduler: cluster.Scheduler{
			Namespace: "volcano-system",
			Name:      "volcano-scheduler-0",
		},
		Nodes: map[string]cluster.CacheNode{
			"node-a": {
				Name:        "node-a",
				Idle:        map[string]float64{"cpu": 2, "pods": 10},
				Allocatable: map[string]float64{"cpu": 16, "pods": 100},
			},
		},
	}

	result := Evaluate(Input{
		Pod:   pod,
		Nodes: []corev1.Node{node},
		Dump:  dump,
	})

	if !result.CacheCheck.Passed || result.AllocationCheck.Determinate {
		t.Fatalf("result=%+v", result)
	}

	if result.PluginCheck.Passed || result.PluginCheck.Determinate {
		t.Fatalf("plugin check=%+v", result.PluginCheck)
	}
}

func TestEvaluateCaptureFailureIsUnknown(t *testing.T) {
	result := Evaluate(Input{
		Pod:        testPendingPod(),
		Nodes:      []corev1.Node{testReadyNode()},
		CaptureErr: errors.New("capture failed"),
	})

	if result.CacheCheck.Determinate || !strings.Contains(result.CacheCheck.Reason, "capture failed") {
		t.Fatalf("cache check=%+v", result.CacheCheck)
	}
}

func TestEvaluateReportsNodeCollectionErrorAsUnknown(t *testing.T) {
	result := Evaluate(Input{
		Pod:      &corev1.Pod{},
		NodesErr: errors.New("informer cache unavailable"),
		Dump: cluster.CacheDump{
			Nodes: map[string]cluster.CacheNode{},
		},
	})

	if result.AllocationCheck.Determinate || result.AllocationCheck.Passed {
		t.Fatalf("allocation check=%+v", result.AllocationCheck)
	}

	if !strings.Contains(result.AllocationCheck.Reason, "informer cache unavailable") {
		t.Fatalf("allocation reason=%q", result.AllocationCheck.Reason)
	}
}

func testPendingPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-a",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
}

func testReadyNode() corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-a",
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:  resource.MustParse("16"),
				corev1.ResourcePods: resource.MustParse("110"),
			},
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
}

func findNodeCheck(t *testing.T, result model.NodeResult, id string) model.Check {
	t.Helper()

	for _, check := range result.Checks {
		if check.ID == id {
			return check
		}
	}

	t.Fatalf("missing check %q in %+v", id, result.Checks)

	return model.Check{}
}
