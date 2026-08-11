package cluster

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestBuildProportionQueueSnapshotFromCacheDump(t *testing.T) {
	dump := CacheDump{
		Nodes: map[string]CacheNode{
			"node-a": {
				Allocatable: map[string]float64{
					"cpu":            100,
					"memory":         100 << 30,
					"nvidia.com/gpu": 8,
				},
			},
		},
		Jobs: map[string]CacheJob{
			"default/pg-running": {
				Namespace: "default",
				Name:      "pg-running",
				Queue:     "default",
				Tasks: []CacheTask{
					{
						Status: "Running",
						Resreq: map[string]float64{
							"cpu":            30,
							"nvidia.com/gpu": 5,
						},
					},
				},
			},
			"default/pg-inqueue": {
				Namespace: "default",
				Name:      "pg-inqueue",
				Queue:     "default",
				Tasks: []CacheTask{
					{
						Status: "Pending",
						Resreq: map[string]float64{
							"cpu":            5,
							"nvidia.com/gpu": 1,
						},
					},
				},
			},
			"default/other-queue": {
				Namespace: "default",
				Name:      "other-queue",
				Queue:     "research",
				Tasks: []CacheTask{
					{
						Status: "Running",
						Resreq: map[string]float64{"cpu": 20},
					},
				},
			},
		},
	}
	queues := []Queue{
		{
			Name:      "default",
			Guarantee: resourceList("cpu", "10"),
			Capability: resourceList(
				"cpu",
				"90",
				"memory",
				"90Gi",
				"nvidia.com/gpu",
				"8",
			),
		},
		{
			Name:      "research",
			Guarantee: resourceList("cpu", "20"),
		},
	}
	podGroups := []PodGroup{
		{
			Namespace:    "default",
			Name:         "pg-running",
			Phase:        "Running",
			MinMember:    1,
			MinResources: resourceList("cpu", "20", "nvidia.com/gpu", "4"),
		},
		{
			Namespace:    "default",
			Name:         "pg-inqueue",
			Phase:        "Inqueue",
			MinResources: resourceList("cpu", "10", "nvidia.com/gpu", "2"),
		},
	}

	snapshot, err := BuildProportionQueueSnapshot(dump, queues, podGroups, "default")
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.Source != queueCacheDumpSource {
		t.Fatalf("source=%q", snapshot.Source)
	}

	assertQueueSnapshotValue(t, snapshot.Resources["cpu"].Capability, 80)
	assertQueueSnapshotValue(t, snapshot.Resources["cpu"].Allocated, 30)
	assertQueueSnapshotValue(t, snapshot.Resources["cpu"].Request, 35)
	assertQueueSnapshotValue(t, snapshot.Resources["cpu"].Inqueue, 10)
	assertQueueSnapshotValue(t, snapshot.Resources["cpu"].Elastic, 10)
	assertQueueSnapshotValue(t, snapshot.Resources["memory"].Capability, 90<<30)
	assertQueueSnapshotValue(t, snapshot.Resources["nvidia.com/gpu"].Capability, 8)
	assertQueueSnapshotValue(t, snapshot.Resources["nvidia.com/gpu"].Allocated, 5)
	assertQueueSnapshotValue(t, snapshot.Resources["nvidia.com/gpu"].Request, 6)
	assertQueueSnapshotValue(t, snapshot.Resources["nvidia.com/gpu"].Inqueue, 2)
	assertQueueSnapshotValue(t, snapshot.Resources["nvidia.com/gpu"].Elastic, 1)
}

func resourceList(nameValuePairs ...string) corev1.ResourceList {
	result := corev1.ResourceList{}

	for index := 0; index < len(nameValuePairs); index += 2 {
		result[corev1.ResourceName(nameValuePairs[index])] = resource.MustParse(nameValuePairs[index+1])
	}

	return result
}

func assertQueueSnapshotValue(t *testing.T, got *float64, want float64) {
	t.Helper()

	if got == nil || *got != want {
		t.Fatalf("metric=%v want=%v", got, want)
	}
}
