package cluster

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestGetVolcanoSchedulerConfigurationUsesMountedConfigMapFromInformer(t *testing.T) {
	pod := schedulerPod("custom-scheduler-control-plane", "volcanosh/vc-scheduler:v1.12.0")
	pod.Namespace = "custom-volcano-system"
	pod.Spec.Containers[1].Args = []string{"--scheduler-conf=/etc/volcano/policy.yaml"}
	pod.Spec.Containers[1].VolumeMounts = []corev1.VolumeMount{
		{
			Name:      "runtime-policy",
			MountPath: "/etc/volcano",
		},
	}
	pod.Spec.Volumes = []corev1.Volume{
		{
			Name: "runtime-policy",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "release-scheduler-policy",
					},
					Items: []corev1.KeyToPath{
						{
							Key:  "scheduler-policy",
							Path: "policy.yaml",
						},
					},
				},
			},
		},
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       pod.Namespace,
			Name:            "release-scheduler-policy",
			UID:             types.UID("policy-uid"),
			ResourceVersion: "17",
		},
		Data: map[string]string{
			"scheduler-policy": `
actions: "enqueue, allocate, backfill"
tiers:
- plugins:
  - name: priority
  - name: capacity
    enableJobEnqueued: false
    arguments:
      capacity.default: 10
- plugins:
  - name: predicates
    enablePredicate: true
configurations:
- name: init-params
  arguments:
    mode: strict
metrics:
  interval: 30s
`,
		},
	}
	kube := fake.NewSimpleClientset(pod, configMap)
	manager := startTestClient(t, kube)

	configuration, err := manager.GetVolcanoSchedulerConfiguration(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !configuration.Determinate {
		t.Fatalf("configuration=%+v", configuration)
	}

	if configuration.ConfigMapNamespace != pod.Namespace ||
		configuration.SchedulerUID != string(pod.UID) ||
		configuration.ConfigMapName != configMap.Name ||
		configuration.ConfigMapKey != "scheduler-policy" ||
		configuration.ConfigMapUID != "policy-uid" ||
		configuration.ConfigMapResourceVersion != "17" {
		t.Fatalf("configuration source=%+v", configuration)
	}

	if strings.Join(configuration.Actions, ",") != "enqueue,allocate,backfill" {
		t.Fatalf("actions=%v", configuration.Actions)
	}

	if len(configuration.Tiers) != 2 ||
		len(configuration.Tiers[0].Plugins) != 2 ||
		configuration.Tiers[0].Plugins[1].Name != "capacity" ||
		configuration.Tiers[1].Plugins[0].Name != "predicates" {
		t.Fatalf("tiers=%+v", configuration.Tiers)
	}

	if enabled, ok := configuration.Tiers[0].Plugins[1].Options["enableJobEnqueued"].(bool); !ok || enabled {
		t.Fatalf("capacity options=%v", configuration.Tiers[0].Plugins[1].Options)
	}

	if len(configuration.Configurations) != 1 ||
		configuration.Configurations[0].Name != "init-params" ||
		configuration.Configurations[0].Arguments["mode"] != "strict" {
		t.Fatalf("configurations=%+v", configuration.Configurations)
	}

	readsBefore := configMapReadCount(kube.Actions())

	for index := 0; index < 3; index++ {
		if _, err := manager.GetVolcanoSchedulerConfiguration(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	readsAfter := configMapReadCount(kube.Actions())
	if readsAfter != readsBefore {
		t.Fatalf(
			"cached configuration reads added ConfigMap API actions: before=%d after=%d actions=%v",
			readsBefore,
			readsAfter,
			kube.Actions(),
		)
	}
}

func TestGetVolcanoSchedulerConfigurationObservesConfigMapWatchUpdate(t *testing.T) {
	pod, configMap := schedulerConfigurationObjects("enqueue, allocate")
	kube := fake.NewSimpleClientset(pod, configMap)
	manager := startTestClient(t, kube)

	configuration, err := manager.GetVolcanoSchedulerConfiguration(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !configuration.Determinate || strings.Join(configuration.Actions, ",") != "enqueue,allocate" {
		t.Fatalf("configuration before update=%+v", configuration)
	}

	updated := configMap.DeepCopy()
	updated.Data["volcano-scheduler.conf"] = schedulerConfigurationYAML("enqueue, allocate, backfill")

	if _, err := kube.CoreV1().ConfigMaps(updated.Namespace).Update(
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
			configuration, err := manager.GetVolcanoSchedulerConfiguration(ctx)
			if err != nil {
				return false, err
			}

			return configuration.Determinate &&
				strings.Join(configuration.Actions, ",") == "enqueue,allocate,backfill", nil
		},
	)
	if err != nil {
		t.Fatalf("scheduler ConfigMap informer did not observe update: %v", err)
	}
}

func TestGetVolcanoSchedulerConfigurationParsesShellWrappedFlagAndSubPath(t *testing.T) {
	pod := schedulerPod("scheduler", "volcanosh/vc-scheduler:v1")
	container := &pod.Spec.Containers[1]
	container.Command = []string{"/bin/sh"}
	container.Args = []string{
		"-c",
		"/vc-scheduler --scheduler-conf=/etc/runtime/scheduler.yaml --leader-elect=false",
	}
	container.VolumeMounts = []corev1.VolumeMount{
		{
			Name:      "policy",
			MountPath: "/etc/runtime/scheduler.yaml",
			SubPath:   "nested/runtime.yaml",
		},
	}
	pod.Spec.Volumes = []corev1.Volume{
		{
			Name: "policy",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "custom-policy"},
					Items: []corev1.KeyToPath{
						{Key: "actual-key", Path: "nested/runtime.yaml"},
					},
				},
			},
		},
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: "custom-policy"},
		Data: map[string]string{
			"actual-key": schedulerConfigurationYAML("enqueue, allocate"),
		},
	}
	manager := startTestClient(t, fake.NewSimpleClientset(pod, configMap))

	configuration, err := manager.GetVolcanoSchedulerConfiguration(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if !configuration.Determinate || configuration.ConfigMapKey != "actual-key" {
		t.Fatalf("configuration=%+v", configuration)
	}
}

func TestGetVolcanoSchedulerConfigurationReportsBuiltInDefaultSource(t *testing.T) {
	pod := schedulerPod("scheduler", "volcanosh/vc-scheduler:v1")
	manager := startTestClient(t, fake.NewSimpleClientset(pod))

	configuration, err := manager.GetVolcanoSchedulerConfiguration(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if configuration.Determinate || !strings.Contains(configuration.Reason, "DefaultSchedulerConf") {
		t.Fatalf("configuration=%+v", configuration)
	}
}

func TestParseSchedulerConfigurationRejectsInvalidShape(t *testing.T) {
	tests := []string{
		"actions: ''",
		"actions: 'enqueue,'",
		"actions: enqueue\ntiers:\n- plugins:\n  - arguments: {}",
		"actions: enqueue\ntiers:\n- plugins:\n  - name: predicates\n    arguments: invalid",
	}

	for _, content := range tests {
		if _, _, _, _, err := parseSchedulerConfiguration(content); err == nil {
			t.Fatalf("content %q unexpectedly parsed", content)
		}
	}
}

func schedulerConfigurationObjects(actions string) (*corev1.Pod, *corev1.ConfigMap) {
	pod := schedulerPod("scheduler", "volcanosh/vc-scheduler:v1")
	pod.Spec.Containers[1].Args = []string{
		"--scheduler-conf=/volcano.scheduler/volcano-scheduler.conf",
	}
	pod.Spec.Containers[1].VolumeMounts = []corev1.VolumeMount{
		{Name: "scheduler-config", MountPath: "/volcano.scheduler"},
	}
	pod.Spec.Volumes = []corev1.Volume{
		{
			Name: "scheduler-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "volcano-scheduler-configmap",
					},
				},
			},
		},
	}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pod.Namespace,
			Name:      "volcano-scheduler-configmap",
		},
		Data: map[string]string{
			"volcano-scheduler.conf": schedulerConfigurationYAML(actions),
		},
	}

	return pod, configMap
}

func schedulerConfigurationYAML(actions string) string {
	return "actions: \"" + actions + "\"\n" +
		"tiers:\n" +
		"- plugins:\n" +
		"  - name: gang\n" +
		"- plugins:\n" +
		"  - name: predicates\n"
}

func configMapReadCount(actions []k8stesting.Action) int {
	count := 0

	for _, action := range actions {
		if action.GetResource().Resource != "configmaps" {
			continue
		}

		if action.GetVerb() == "get" || action.GetVerb() == "list" {
			count++
		}
	}

	return count
}
