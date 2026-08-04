package filter

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestResourceListValues(t *testing.T) {
	values := resourceListValues(corev1.ResourceList{
		corev1.ResourceCPU:                    resource.MustParse("500m"),
		corev1.ResourceMemory:                 resource.MustParse("2Gi"),
		corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("2"),
	})

	if values["cpu"] != .5 {
		t.Fatalf("cpu=%v", values["cpu"])
	}

	if values["memory"] != 2*1024*1024*1024 {
		t.Fatalf("memory=%v", values["memory"])
	}

	if values["nvidia.com/gpu"] != 2 {
		t.Fatalf("gpu=%v", values["nvidia.com/gpu"])
	}
}

func TestTaintsTolerated(t *testing.T) {
	taints := []corev1.Taint{
		{
			Key:    "dedicated",
			Value:  "gpu",
			Effect: corev1.TaintEffectNoSchedule,
		},
	}

	if taintsTolerated(taints, nil) {
		t.Fatal("untolerated taint passed")
	}

	existsToleration := []corev1.Toleration{
		{
			Key:      "dedicated",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	if !taintsTolerated(taints, existsToleration) {
		t.Fatal("Exists toleration failed")
	}

	equalToleration := []corev1.Toleration{
		{
			Key:      "dedicated",
			Operator: corev1.TolerationOpEqual,
			Value:    "gpu",
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	if !taintsTolerated(taints, equalToleration) {
		t.Fatal("Equal toleration failed")
	}
}

func TestPodRequestsIncludesInitAndOverhead(t *testing.T) {
	pod := &corev1.Pod{
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
			InitContainers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
			Overhead: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("100m"),
			},
		},
	}

	requests := podRequests(pod)

	if requests["cpu"] != 2.1 {
		t.Fatalf("cpu=%v", requests["cpu"])
	}

	if requests["pods"] != 1 {
		t.Fatalf("pods=%v", requests["pods"])
	}
}

func TestFitsExtendedResource(t *testing.T) {
	ok, _ := fits(
		map[string]float64{"cpu": 2, "nvidia.com/gpu": 1},
		map[string]float64{"cpu": 4, "nvidia.com/gpu": 1},
	)

	if !ok {
		t.Fatal("resources should fit")
	}

	ok, _ = fits(map[string]float64{"cpu": 5}, map[string]float64{"cpu": 4})

	if ok {
		t.Fatal("oversized request should fail")
	}
}
