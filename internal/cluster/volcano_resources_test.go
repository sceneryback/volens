package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func TestVolcanoInformerInitialSyncCachedReadsAndWatchUpdate(t *testing.T) {
	podGroup := testPodGroup("default", "job-a")
	queue := testQueue("training")
	volcano := newFakeVolcanoClient(podGroup, queue)
	manager := startTestClientWithDynamic(t, fake.NewSimpleClientset(), volcano)

	gotPodGroup, err := manager.GetPodGroup(context.Background(), "default", "job-a")
	if err != nil {
		t.Fatal(err)
	}

	if gotPodGroup.Queue != "training" || gotPodGroup.MinMember != 2 || gotPodGroup.Phase != "Pending" {
		t.Fatalf("PodGroup=%+v", gotPodGroup)
	}

	if gotPodGroup.MinTaskMember["worker"] != 2 {
		t.Fatalf("PodGroup MinTaskMember=%v", gotPodGroup.MinTaskMember)
	}

	if gotPodGroup.MinResources.Cpu().String() != "2" || gotPodGroup.MinResources.Memory().String() != "4Gi" {
		t.Fatalf("PodGroup MinResources=%v", gotPodGroup.MinResources)
	}

	if len(gotPodGroup.Conditions) != 1 || gotPodGroup.Conditions[0].Reason != "NotEnoughResources" {
		t.Fatalf("PodGroup Conditions=%v", gotPodGroup.Conditions)
	}

	gotQueue, err := manager.GetQueue(context.Background(), "training")
	if err != nil {
		t.Fatal(err)
	}

	if gotQueue.State != "Open" || gotQueue.Weight != 3 || gotQueue.Capability.Cpu().String() != "100" {
		t.Fatalf("Queue=%+v", gotQueue)
	}

	actionsBefore := len(volcano.Actions())

	gotPodGroup.MinTaskMember["worker"] = 99
	gotPodGroup.MinResources[corev1.ResourceCPU] = gotQueue.Capability[corev1.ResourceCPU]

	secondPodGroup, err := manager.GetPodGroup(context.Background(), "default", "job-a")
	if err != nil {
		t.Fatal(err)
	}

	if secondPodGroup.MinTaskMember["worker"] != 2 || secondPodGroup.MinResources.Cpu().String() != "2" {
		t.Fatalf("PodGroup informer cache was mutated: %+v", secondPodGroup)
	}

	if _, err := manager.GetQueue(context.Background(), "training"); err != nil {
		t.Fatal(err)
	}

	if actionsAfter := len(volcano.Actions()); actionsAfter != actionsBefore {
		t.Fatalf(
			"cached reads added dynamic API actions: before=%d after=%d actions=%v",
			actionsBefore,
			actionsAfter,
			volcano.Actions(),
		)
	}

	updated := podGroup.DeepCopy()

	if err := unstructured.SetNestedField(updated.Object, "Inqueue", "status", "phase"); err != nil {
		t.Fatal(err)
	}

	if _, err := volcano.Resource(podGroupGVR).Namespace("default").Update(
		context.Background(),
		updated,
		metav1.UpdateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	err = wait.PollUntilContextTimeout(
		context.Background(),
		10*time.Millisecond,
		2*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			current, err := manager.GetPodGroup(ctx, "default", "job-a")
			if err != nil {
				return false, err
			}

			return current.Phase == "Inqueue", nil
		},
	)
	if err != nil {
		t.Fatalf("PodGroup cache did not observe watch update: %v", err)
	}
}

func TestListPodsForPodGroupUsesInformerIndexAndReturnsCopies(t *testing.T) {
	jobLabelPod := testPod("default", "job-label", corev1.PodPending)
	jobLabelPod.Labels = map[string]string{"volcano.sh/job-name": "job-a"}
	groupAnnotationPod := testPod("default", "group-annotation", corev1.PodPending)
	groupAnnotationPod.Annotations = map[string]string{podGroupNameAnnotation: "job-a"}
	bothPod := testPod("default", "both", corev1.PodPending)
	bothPod.Labels = map[string]string{"volcano.sh/job-name": "job-a"}
	bothPod.Annotations = map[string]string{podGroupNameAnnotation: "job-a"}
	otherPod := testPod("default", "other", corev1.PodPending)
	otherPod.Annotations = map[string]string{podGroupNameAnnotation: "job-b"}
	otherNamespacePod := testPod("other", "other-namespace", corev1.PodPending)
	otherNamespacePod.Annotations = map[string]string{podGroupNameAnnotation: "job-a"}

	kube := fake.NewSimpleClientset(
		jobLabelPod,
		groupAnnotationPod,
		bothPod,
		otherPod,
		otherNamespacePod,
	)
	manager := startTestClient(t, kube)

	actionsBefore := len(kube.Actions())
	pods, err := manager.ListPodsForPodGroup(context.Background(), "default", "job-a")
	if err != nil {
		t.Fatal(err)
	}

	if len(pods) != 2 || pods[0].Name != "both" || pods[1].Name != "group-annotation" {
		t.Fatalf("Pods=%v", podNames(pods))
	}

	if actionsAfter := len(kube.Actions()); actionsAfter != actionsBefore {
		t.Fatalf("cached PodGroup Pod read added API actions: before=%d after=%d", actionsBefore, actionsAfter)
	}

	pods[0].Labels["mutated"] = "true"

	second, err := manager.ListPodsForPodGroup(context.Background(), "default", "job-a")
	if err != nil {
		t.Fatal(err)
	}

	if second[0].Labels["mutated"] != "" {
		t.Fatalf("Pod informer cache was mutated: labels=%v", second[0].Labels)
	}
}

func TestVolcanoResourceMethodsReportUnavailableWithoutDynamicClient(t *testing.T) {
	manager := startTestClient(t, fake.NewSimpleClientset())

	if _, err := manager.GetPodGroup(context.Background(), "default", "job-a"); !errors.Is(err, ErrVolcanoResourceCacheUnavailable) {
		t.Fatalf("GetPodGroup error=%v", err)
	}

	if _, err := manager.GetQueue(context.Background(), "default"); !errors.Is(err, ErrVolcanoResourceCacheUnavailable) {
		t.Fatalf("GetQueue error=%v", err)
	}
}

func TestPodGroupQuantityParsingReturnsErrorForInvalidValue(t *testing.T) {
	podGroup := testPodGroup("default", "job-a")

	if err := unstructured.SetNestedField(
		podGroup.Object,
		map[string]any{"cpu": []any{"invalid"}},
		"spec",
		"minResources",
	); err != nil {
		t.Fatal(err)
	}

	if _, err := podGroupFromUnstructured(podGroup); err == nil {
		t.Fatal("podGroupFromUnstructured accepted an invalid quantity value")
	}
}

func newFakeVolcanoClient(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			podGroupGVR: "PodGroupList",
			queueGVR:    "QueueList",
		},
		objects...,
	)
}

func startTestClientWithDynamic(
	t *testing.T,
	kube *fake.Clientset,
	volcano *dynamicfake.FakeDynamicClient,
) *Client {
	t.Helper()

	manager, err := newClientWithDynamic(kube, volcano, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := manager.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}

	t.Cleanup(func() {
		cancel()
		manager.Shutdown()
	})

	return manager
}

func testPodGroup(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "scheduling.volcano.sh/v1beta1",
			"kind":       "PodGroup",
			"metadata": map[string]any{
				"namespace":       namespace,
				"name":            name,
				"uid":             string(types.UID("podgroup-uid")),
				"resourceVersion": "1",
			},
			"spec": map[string]any{
				"queue":     "training",
				"minMember": int64(2),
				"minTaskMember": map[string]any{
					"worker": int64(2),
				},
				"minResources": map[string]any{
					"cpu":    "2",
					"memory": "4Gi",
				},
			},
			"status": map[string]any{
				"phase":   "Pending",
				"running": int64(0),
				"conditions": []any{
					map[string]any{
						"type":               "UnschedulableType",
						"status":             "True",
						"reason":             "NotEnoughResources",
						"message":            "insufficient resources",
						"lastTransitionTime": "2026-08-02T10:00:00Z",
					},
				},
			},
		},
	}
}

func testQueue(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "scheduling.volcano.sh/v1beta1",
			"kind":       "Queue",
			"metadata": map[string]any{
				"name":            name,
				"uid":             string(types.UID("queue-uid")),
				"resourceVersion": "1",
			},
			"spec": map[string]any{
				"weight":      int64(3),
				"reclaimable": true,
				"capability": map[string]any{
					"cpu":    int64(100),
					"memory": "1Ti",
				},
			},
			"status": map[string]any{
				"state":   "Open",
				"pending": int64(1),
				"allocated": map[string]any{
					"cpu": "20",
				},
			},
		},
	}
}

func podNames(pods []corev1.Pod) []string {
	result := make([]string, 0, len(pods))

	for i := range pods {
		result = append(result, pods[i].Name)
	}

	return result
}
