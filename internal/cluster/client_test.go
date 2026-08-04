package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestImageTag(t *testing.T) {
	tests := map[string]string{
		"volcanosh/vc-scheduler:v1.11.0":           "v1.11.0",
		"registry:5000/team/scheduler:release-1.9": "release-1.9",
		"volcanosh/vc-scheduler":                   "latest",
		"volcanosh/vc-scheduler:v1.10@sha256:abc":  "v1.10",
	}

	for input, want := range tests {
		if got := imageTag(input); got != want {
			t.Errorf("imageTag(%q)=%q, want %q", input, got, want)
		}
	}
}

func TestBoundedSchedulerLogTail(t *testing.T) {
	tests := map[int64]int64{
		-1:                           defaultSchedulerLogTail,
		0:                            defaultSchedulerLogTail,
		1:                            1,
		maximumSchedulerLogTail:      maximumSchedulerLogTail,
		maximumSchedulerLogTail + 10: maximumSchedulerLogTail,
	}

	for input, want := range tests {
		if got := boundedSchedulerLogTail(input); got != want {
			t.Errorf("boundedSchedulerLogTail(%d)=%d, want %d", input, got, want)
		}
	}
}

func TestSchedulerRuntimeConfigurationParsesRepeatedNamesAndDefaultQueue(t *testing.T) {
	container := &corev1.Container{
		Command: []string{"/vc-scheduler"},
		Args: []string{
			"--scheduler-name=batch",
			"--scheduler-name",
			"batch-secondary",
			"--default-queue=batch-default",
			"--lock-object-namespace=custom-locks",
			"--leader-elect=false",
		},
	}

	names, queue, determinate, reason := schedulerRuntimeConfiguration(container)
	if !determinate || queue != "batch-default" ||
		len(names) != 2 || names[0] != "batch" || names[1] != "batch-secondary" {
		t.Fatalf(
			"names=%v queue=%q determinate=%t reason=%q",
			names,
			queue,
			determinate,
			reason,
		)
	}

	options := parseSchedulerRuntimeOptions(container)
	if !options.determinate || options.leaderElection ||
		options.lockObjectNamespace != "custom-locks" {
		t.Fatalf("options=%+v", options)
	}
}

func TestSchedulerRuntimeConfigurationUsesOfficialDefaults(t *testing.T) {
	names, queue, determinate, reason := schedulerRuntimeConfiguration(
		&corev1.Container{Command: []string{"/vc-scheduler"}},
	)

	if !determinate || queue != defaultVolcanoQueue ||
		len(names) != 1 || names[0] != defaultVolcanoSchedulerName {
		t.Fatalf(
			"names=%v queue=%q determinate=%t reason=%q",
			names,
			queue,
			determinate,
			reason,
		)
	}
}

func TestSchedulerRuntimeConfigurationRejectsUninspectableFlags(t *testing.T) {
	tests := []corev1.Container{
		{
			Command: []string{"sh", "-exc"},
			Args:    []string{"/vc-scheduler --scheduler-name=batch"},
		},
		{
			Command: []string{"/vc-scheduler"},
			Args:    []string{"--default-queue=$(DEFAULT_QUEUE)"},
		},
	}

	for _, container := range tests {
		_, _, determinate, reason := schedulerRuntimeConfiguration(&container)
		if determinate || reason == "" {
			t.Fatalf("container=%+v determinate=%t reason=%q", container, determinate, reason)
		}
	}
}

func TestLoadRESTConfigUsesStandardKubeconfigEnvironment(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	path := filepath.Join(t.TempDir(), "kubeconfig")
	content := []byte(`apiVersion: v1
kind: Config
clusters:
  - name: local
    cluster:
      server: https://127.0.0.1:6443
contexts:
  - name: local
    context:
      cluster: local
      user: local
current-context: local
users:
  - name: local
    user: {}
`)

	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("KUBECONFIG", path)

	config, err := loadRESTConfig()
	if err != nil {
		t.Fatal(err)
	}

	if config.Host != "https://127.0.0.1:6443" {
		t.Fatalf("host=%q", config.Host)
	}
}

func TestInformerInitialSyncAndCachedReads(t *testing.T) {
	pendingB := testPod("z", "pod-b", corev1.PodPending)
	pendingB.Spec.SchedulerName = "volcano"
	pendingA := testPod("a", "pod-a", corev1.PodPending)
	running := testPod("a", "running", corev1.PodRunning)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-a",
		},
	}

	kube := fake.NewSimpleClientset(
		pendingB,
		pendingA,
		running,
		node,
		schedulerPod("volcano-scheduler-0", "image:v1"),
	)
	manager := startTestClient(t, kube)

	pods, err := manager.ListPendingPods(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(pods) != 2 {
		t.Fatalf("pending pods=%v", pods)
	}

	if pods[0].Namespace != "a" || pods[0].Name != "pod-a" {
		t.Fatalf("first pending pod=%+v", pods[0])
	}

	if pods[1].Namespace != "z" || pods[1].Name != "pod-b" || pods[1].Scheduler != "volcano" {
		t.Fatalf("second pending pod=%+v", pods[1])
	}

	gotPod, err := manager.GetPod(context.Background(), "a", "pod-a")
	if err != nil {
		t.Fatal(err)
	}

	if gotPod.Name != "pod-a" {
		t.Fatalf("pod=%+v", gotPod)
	}

	nodes, err := manager.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(nodes) != 1 || nodes[0].Name != "node-a" {
		t.Fatalf("nodes=%+v", nodes)
	}

	gotNode, err := manager.GetNode(context.Background(), "node-a")
	if err != nil {
		t.Fatal(err)
	}

	if gotNode.Name != "node-a" {
		t.Fatalf("node=%+v", gotNode)
	}

	scheduler, err := manager.GetVolcanoScheduler(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if scheduler.Name != "volcano-scheduler-0" || scheduler.Tag != "v1" ||
		!scheduler.ConfigurationDeterminate || scheduler.DefaultQueue != "default" ||
		len(scheduler.SchedulerNames) != 1 || scheduler.SchedulerNames[0] != "volcano" {
		t.Fatalf("scheduler=%+v", scheduler)
	}

	readsBefore := cachedResourceReadCount(kube.Actions())

	for i := 0; i < 3; i++ {
		if _, err := manager.ListPendingPods(context.Background()); err != nil {
			t.Fatal(err)
		}

		if _, err := manager.GetPod(context.Background(), "a", "pod-a"); err != nil {
			t.Fatal(err)
		}

		if _, err := manager.ListNodes(context.Background()); err != nil {
			t.Fatal(err)
		}

		if _, err := manager.GetNode(context.Background(), "node-a"); err != nil {
			t.Fatal(err)
		}

		if _, err := manager.GetVolcanoScheduler(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	readsAfter := cachedResourceReadCount(kube.Actions())
	if readsAfter != readsBefore {
		t.Fatalf("cached reads added Kubernetes API actions: before=%d after=%d actions=%v", readsBefore, readsAfter, kube.Actions())
	}
}

func TestInformerWatchUpdatesPendingPodIndex(t *testing.T) {
	pod := testPod("default", "pending", corev1.PodPending)
	kube := fake.NewSimpleClientset(pod)
	manager := startTestClient(t, kube)

	pods, err := manager.ListPendingPods(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(pods) != 1 {
		t.Fatalf("pending pods before update=%v", pods)
	}

	updated := pod.DeepCopy()
	updated.Status.Phase = corev1.PodRunning

	if _, err := kube.CoreV1().Pods(updated.Namespace).Update(
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
			pods, err := manager.ListPendingPods(ctx)
			if err != nil {
				return false, err
			}

			return len(pods) == 0, nil
		},
	)
	if err != nil {
		t.Fatalf("Pending Pod index did not observe watch update: %v", err)
	}
}

func TestGetVolcanoSchedulerReportsNotDiscoveredSentinel(t *testing.T) {
	manager := startTestClient(t, fake.NewSimpleClientset())

	_, err := manager.GetVolcanoScheduler(context.Background())
	if !errors.Is(err, ErrVolcanoSchedulerNotDiscovered) {
		t.Fatalf("err=%v", err)
	}
}

func TestGetVolcanoSchedulerReportsNotReadyForIdentifiedPod(t *testing.T) {
	pod := schedulerPod("custom-name", "volcanosh/vc-scheduler:v1")
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	manager := startTestClient(t, fake.NewSimpleClientset(pod))

	_, err := manager.GetVolcanoScheduler(context.Background())
	if !errors.Is(err, ErrVolcanoSchedulerNotReady) {
		t.Fatalf("err=%v", err)
	}
}

func TestGetVolcanoSchedulerUsesStandardLabelWithCustomPodName(t *testing.T) {
	pod := schedulerPod("custom-control-plane", "volcanosh/vc-scheduler:v1.12.0")
	manager := startTestClient(t, fake.NewSimpleClientset(pod))

	scheduler, err := manager.GetVolcanoScheduler(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if scheduler.Name != pod.Name || scheduler.Tag != "v1.12.0" {
		t.Fatalf("scheduler=%+v", scheduler)
	}
}

func TestSchedulerPodIndexObservesImageUpdate(t *testing.T) {
	pod := schedulerPod("volcano-scheduler-a", "volcanosh/vc-scheduler:v1")
	kube := fake.NewSimpleClientset(pod)
	manager := startTestClient(t, kube)

	updated := pod.DeepCopy()
	updated.Spec.Containers[1].Image = "volcanosh/vc-scheduler:v2"

	if _, err := kube.CoreV1().Pods(updated.Namespace).Update(
		context.Background(),
		updated,
		metav1.UpdateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	err := wait.PollUntilContextTimeout(
		context.Background(),
		10*time.Millisecond,
		2*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			scheduler, err := manager.GetVolcanoScheduler(ctx)
			if err != nil {
				return false, err
			}

			return scheduler.Tag == "v2", nil
		},
	)
	if err != nil {
		t.Fatalf("scheduler Pod index did not observe image update: %v", err)
	}
}

func TestGetVolcanoSchedulerDoesNotTreatExporterAsScheduler(t *testing.T) {
	pod := schedulerPod("volcano-scheduler-exporter", "example/volcano-scheduler-exporter:v1")
	pod.Labels["app"] = "volcano-scheduler-exporter"
	pod.Spec.Containers[1].Name = "vc-scheduler-metrics"
	pod.Spec.Containers[1].Image = "example/vc-scheduler-metrics:v1"
	manager := startTestClient(t, fake.NewSimpleClientset(pod))

	_, err := manager.GetVolcanoScheduler(context.Background())
	if !errors.Is(err, ErrVolcanoSchedulerNotDiscovered) {
		t.Fatalf("err=%v", err)
	}
}

func TestInformerResultsAreDeepCopies(t *testing.T) {
	pod := testPod("default", "pod-a", corev1.PodPending)
	pod.Labels = map[string]string{"source": "cache"}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-a",
			Labels: map[string]string{"source": "cache"},
		},
	}

	manager := startTestClient(t, fake.NewSimpleClientset(pod, node))

	firstPod, err := manager.GetPod(context.Background(), "default", "pod-a")
	if err != nil {
		t.Fatal(err)
	}

	firstPod.Labels["source"] = "mutated"
	firstPod.Labels["new"] = "value"

	secondPod, err := manager.GetPod(context.Background(), "default", "pod-a")
	if err != nil {
		t.Fatal(err)
	}

	if secondPod.Labels["source"] != "cache" || secondPod.Labels["new"] != "" {
		t.Fatalf("Pod informer cache was mutated: labels=%v", secondPod.Labels)
	}

	firstNodes, err := manager.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	firstNodes[0].Labels["source"] = "mutated"
	firstNodes[0].Labels["new"] = "value"

	secondNode, err := manager.GetNode(context.Background(), "node-a")
	if err != nil {
		t.Fatal(err)
	}

	if secondNode.Labels["source"] != "cache" || secondNode.Labels["new"] != "" {
		t.Fatalf("Node informer cache was mutated: labels=%v", secondNode.Labels)
	}
}

func TestListPodEventsUsesCurrentPodUIDSelector(t *testing.T) {
	pod := testPod("default", "pod-a", corev1.PodPending)
	pod.UID = types.UID("current-pod-uid")
	matching := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "matching",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: "default",
			Name:      "pod-a",
			UID:       pod.UID,
		},
	}

	oldPodEvent := matching.DeepCopy()
	oldPodEvent.Name = "old-pod"
	oldPodEvent.InvolvedObject.UID = types.UID("old-pod-uid")

	kube := fake.NewSimpleClientset(pod, matching, oldPodEvent)
	manager := startTestClient(t, kube)

	actionStart := len(kube.Actions())
	events, err := manager.ListPodEvents(context.Background(), "default", "pod-a")
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 1 || events[0].Name != "matching" {
		t.Fatalf("events=%+v", events)
	}

	listAction := findEventListAction(t, kube.Actions()[actionStart:])
	selector := listAction.GetListRestrictions().Fields
	values := fields.Set{
		"involvedObject.kind": "Pod",
		"involvedObject.name": "pod-a",
		"involvedObject.uid":  "current-pod-uid",
	}

	if selector == nil {
		t.Fatal("Event field selector is nil")
	}

	for field, want := range values {
		got, found := selector.RequiresExactMatch(field)
		if !found || got != want {
			t.Fatalf("Event field selector %q requires %s=%q, want %q", selector, field, got, want)
		}
	}
}

func TestListPodGroupEventsUsesKindAndNameSelector(t *testing.T) {
	matching := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "matching",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "PodGroup",
			Namespace: "default",
			Name:      "job-a",
			UID:       types.UID("job-a-uid"),
		},
	}

	other := matching.DeepCopy()
	other.Name = "other"
	other.InvolvedObject.Name = "job-b"

	kube := fake.NewSimpleClientset(matching, other)
	manager := startTestClient(t, kube)

	actionStart := len(kube.Actions())
	events, err := manager.ListPodGroupEvents(context.Background(), "default", "job-a", "job-a-uid")
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 1 || events[0].Name != "matching" {
		t.Fatalf("events=%+v", events)
	}

	listAction := findEventListAction(t, kube.Actions()[actionStart:])
	selector := listAction.GetListRestrictions().Fields
	values := fields.Set{
		"involvedObject.kind": "PodGroup",
		"involvedObject.name": "job-a",
		"involvedObject.uid":  "job-a-uid",
	}

	for field, want := range values {
		got, found := selector.RequiresExactMatch(field)
		if !found || got != want {
			t.Fatalf("Event field selector %q requires %s=%q, want %q", selector, field, got, want)
		}
	}
}

func TestSchedulerSelectsLeaseHolderAndObservesWatchUpdate(t *testing.T) {
	holder := "volcano-scheduler-b_uid"
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "custom-lock-namespace",
			Name:      "batch",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holder,
		},
	}

	schedulerA := schedulerPod("volcano-scheduler-a", "image:v1")
	schedulerB := schedulerPod("volcano-scheduler-b", "image:v2")
	configureSchedulerLease(schedulerA, "batch", lease.Namespace)
	configureSchedulerLease(schedulerB, "batch", lease.Namespace)
	kube := fake.NewSimpleClientset(schedulerA, schedulerB, lease)
	manager := startTestClient(t, kube)

	scheduler, err := manager.GetVolcanoScheduler(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if scheduler.Name != "volcano-scheduler-b" || scheduler.Container != "volcano-scheduler" || scheduler.Tag != "v2" {
		t.Fatalf("scheduler=%+v", scheduler)
	}

	newHolder := "volcano-scheduler-a_uid"
	updatedLease := lease.DeepCopy()
	updatedLease.Spec.HolderIdentity = &newHolder

	if _, err := kube.CoordinationV1().Leases(updatedLease.Namespace).Update(
		context.Background(),
		updatedLease,
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
			scheduler, err := manager.GetVolcanoScheduler(ctx)
			if err != nil {
				return false, err
			}

			return scheduler.Name == "volcano-scheduler-a" && scheduler.Tag == "v1", nil
		},
	)
	if err != nil {
		t.Fatalf("scheduler leader lookup did not observe Lease update: %v", err)
	}
}

func TestSchedulerDoesNotSelectReadyStandbyWhileLeaseHolderIsNotReady(t *testing.T) {
	holder := "volcano-scheduler-a_uid"
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "custom-lock-namespace",
			Name:      "batch",
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder},
	}
	leader := schedulerPod("volcano-scheduler-a", "image:v1")
	leader.Status.Conditions[0].Status = corev1.ConditionFalse
	standby := schedulerPod("volcano-scheduler-b", "image:v1")
	configureSchedulerLease(leader, "batch", lease.Namespace)
	configureSchedulerLease(standby, "batch", lease.Namespace)
	manager := startTestClient(t, fake.NewSimpleClientset(leader, standby, lease))

	_, err := manager.GetVolcanoScheduler(context.Background())
	if !errors.Is(err, ErrVolcanoSchedulerNotReady) {
		t.Fatalf("err=%v", err)
	}
}

func startTestClient(t *testing.T, kube *fake.Clientset) *Client {
	t.Helper()

	manager, err := newClient(kube, nil)
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

func cachedResourceReadCount(actions []k8stesting.Action) int {
	count := 0

	for _, action := range actions {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			continue
		}

		switch action.GetResource().Resource {
		case "pods", "nodes", "leases":
			count++
		}
	}

	return count
}

func findEventListAction(t *testing.T, actions []k8stesting.Action) k8stesting.ListAction {
	t.Helper()

	for _, action := range actions {
		if !action.Matches("list", "events") {
			continue
		}

		listAction, ok := action.(k8stesting.ListAction)
		if !ok {
			t.Fatalf("Event action has type %T", action)
		}

		return listAction
	}

	t.Fatalf("Event List action not found: actions=%v", actions)

	return nil
}

func testPod(namespace, name string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Status: corev1.PodStatus{
			Phase: phase,
		},
	}
}

func schedulerPod(name, image string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "volcano-system",
			Name:      name,
			UID:       types.UID(name + "-uid"),
			Labels: map[string]string{
				"app": "volcano-scheduler",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "sidecar",
					Image: "sidecar:1",
				},
				{
					Name:  "volcano-scheduler",
					Image: image,
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
}

func configureSchedulerLease(pod *corev1.Pod, schedulerName, namespace string) {
	pod.Spec.Containers[1].Args = []string{
		"--scheduler-name=" + schedulerName,
		"--lock-object-namespace=" + namespace,
	}
}
