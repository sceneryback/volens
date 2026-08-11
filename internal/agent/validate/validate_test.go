package validate

import (
	"strings"
	"testing"

	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEvaluateCommonValidation(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"scheduling.k8s.io/group-name": "job-a",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: "volcano",
			Containers: []corev1.Container{
				{Name: "worker"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	checks := Evaluate(pod, testScheduler(), nil)
	if len(checks) != 7 {
		t.Fatalf("checks=%d", len(checks))
	}

	for _, check := range checks {
		if !check.Passed || !check.Determinate {
			t.Fatalf("common check=%+v", check)
		}
	}
}

func TestEvaluatePodGroupFindsGangTaskShortage(t *testing.T) {
	checks := EvaluatePodGroup(
		"job-a-uid",
		cluster.PodGroup{
			Namespace: "default",
			Name:      "job-a-uid",
			MinMember: 2,
		},
		nil,
		[]corev1.Pod{
			{
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			},
		},
		nil,
	)

	if !checks[0].Passed || checks[1].Passed || checks[1].Determinate ||
		!strings.Contains(checks[1].Reason, "wouldPass=false") {
		t.Fatalf("checks=%+v", checks)
	}
}

func TestEvaluatePodGroupChecksMinTaskMember(t *testing.T) {
	checks := EvaluatePodGroup(
		"job-a-uid",
		cluster.PodGroup{
			Namespace: "default",
			Name:      "job-a-uid",
			MinMember: 2,
			MinTaskMember: map[string]int32{
				"worker": 2,
			},
		},
		nil,
		[]corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{taskSpecAnnotation: "worker"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{taskSpecAnnotation: "other"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			},
		},
		nil,
	)

	if checks[2].Passed || checks[2].Determinate ||
		!strings.Contains(checks[2].Reason, "wouldPass=false") {
		t.Fatalf("minTaskMember check=%+v", checks[2])
	}
}

func TestEvaluateGangTasksExcludesTerminatingPendingButCountsSucceeded(t *testing.T) {
	now := metav1.Now()
	checks := EvaluatePodGroup(
		"job-a-uid",
		cluster.PodGroup{
			Namespace: "default",
			Name:      "job-a-uid",
			MinMember: 2,
		},
		nil,
		[]corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
				Status:     corev1.PodStatus{Phase: corev1.PodPending},
			},
			{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
				Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
			},
		},
		nil,
	)

	if !strings.Contains(checks[1].Reason, "valid tasks=1") ||
		!strings.Contains(checks[1].Reason, "wouldPass=false") {
		t.Fatalf("gang check=%+v", checks[1])
	}
}

func TestEvaluateReportsCommonFailures(t *testing.T) {
	checks := Evaluate(&corev1.Pod{}, testScheduler(), nil)

	for _, index := range []int{0, 1, 5, 6} {
		if checks[index].Passed || !checks[index].Determinate {
			t.Fatalf("check[%d]=%+v", index, checks[index])
		}
	}
}

func TestEvaluateUsesExactConfiguredSchedulerName(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"scheduling.k8s.io/group-name": "job-a",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: "batch",
			Containers:    []corev1.Container{{Name: "worker"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	scheduler := testScheduler()
	scheduler.SchedulerNames = []string{"batch", "batch-secondary"}

	checks := Evaluate(pod, scheduler, nil)
	if !checks[1].Passed || !checks[1].Determinate {
		t.Fatalf("scheduler check=%+v", checks[1])
	}

	pod.Spec.SchedulerName = "contains-batch"
	checks = Evaluate(pod, scheduler, nil)
	if checks[1].Passed || !checks[1].Determinate {
		t.Fatalf("scheduler check=%+v", checks[1])
	}
}

func TestEvaluateMissingReadySchedulerIsKnownCause(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"scheduling.k8s.io/group-name": "job-a",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: "volcano",
			Containers:    []corev1.Container{{Name: "worker"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}

	checks := Evaluate(pod, cluster.Scheduler{}, cluster.ErrVolcanoSchedulerNotReady)
	if checks[1].Passed || !checks[1].Determinate {
		t.Fatalf("scheduler check=%+v", checks[1])
	}
}

func TestEvaluateUndiscoveredSchedulerRemainsUnknown(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"scheduling.k8s.io/group-name": "job-a",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: "volcano",
			Containers:    []corev1.Container{{Name: "worker"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}

	checks := Evaluate(pod, cluster.Scheduler{}, cluster.ErrVolcanoSchedulerNotDiscovered)
	if checks[1].Passed || checks[1].Determinate {
		t.Fatalf("scheduler check=%+v", checks[1])
	}
}

func TestEvaluateAssignedOrTerminatingPodStopsSchedulerDiagnosis(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"scheduling.k8s.io/group-name": "job-a",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: "volcano",
			NodeName:      "node-a",
			Containers:    []corev1.Container{{Name: "worker"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	now := metav1.Now()
	pod.DeletionTimestamp = &now

	checks := Evaluate(pod, testScheduler(), nil)
	if checks[2].Passed || checks[3].Passed {
		t.Fatalf("checks=%+v", checks)
	}
}

func TestEvaluateSchedulingGateIsKnownPendingCause(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"scheduling.k8s.io/group-name": "job-a",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName:   "volcano",
			Containers:      []corev1.Container{{Name: "worker"}},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: "example.com/hold"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}

	checks := Evaluate(pod, testScheduler(), nil)
	if checks[4].Passed || !checks[4].Determinate {
		t.Fatalf("scheduling gate check=%+v", checks[4])
	}
}

func TestPodGroupNameDoesNotUseVolcanoJobLabel(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"volcano.sh/job-name": "job-a",
			},
		},
	}

	if name := PodGroupName(pod); name != "" {
		t.Fatalf("name=%q, want empty without group-name annotation", name)
	}
}

func TestPodGroupNameSupportsGroupAnnotation(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"volcano.sh/job-name": "job-a",
			},
			Annotations: map[string]string{
				"scheduling.k8s.io/group-name": "job-a-uid",
			},
		},
	}

	if name := PodGroupName(pod); name != "job-a-uid" {
		t.Fatalf("name=%q", name)
	}
}

func testScheduler() cluster.Scheduler {
	return cluster.Scheduler{
		Name:                     "volcano-scheduler-0",
		SchedulerNames:           []string{"volcano"},
		DefaultQueue:             "default",
		ConfigurationDeterminate: true,
	}
}
