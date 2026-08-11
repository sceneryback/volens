package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	dynamicinformer "k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	podPhaseIndex               = "volens/pod-phase"
	podGroupIndex               = "volens/pod-group"
	podGroupQueueIndex          = "volens/pod-group-queue"
	podGroupUnspecifiedQueue    = "__volens_unspecified_queue__"
	schedulerPodIndex           = "volens/volcano-scheduler"
	defaultVolcanoSchedulerName = "volcano"
	defaultVolcanoQueue         = "default"
	defaultSchedulerLockNS      = "volcano-system"
	defaultSchedulerLogTail     = int64(200)
	maximumSchedulerLogTail     = int64(1000)
	maximumSchedulerLogResponse = 256 << 10
)

var ErrVolcanoSchedulerNotReady = errors.New("ready Volcano scheduler pod not found")

var ErrVolcanoSchedulerNotDiscovered = errors.New("Volcano scheduler pod could not be discovered")

type PendingPod struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Scheduler string `json:"scheduler"`
}

type Scheduler struct {
	Namespace                string   `json:"namespace"`
	Name                     string   `json:"name"`
	UID                      string   `json:"uid"`
	Container                string   `json:"container"`
	Binary                   string   `json:"binary,omitempty"`
	Image                    string   `json:"image"`
	Tag                      string   `json:"tag"`
	Version                  string   `json:"version,omitempty"`
	GitSHA                   string   `json:"gitSHA,omitempty"`
	BuiltAt                  string   `json:"builtAt,omitempty"`
	GoVersion                string   `json:"goVersion,omitempty"`
	GoOSArch                 string   `json:"goOSArch,omitempty"`
	SchedulerNames           []string `json:"schedulerNames,omitempty"`
	DefaultQueue             string   `json:"defaultQueue,omitempty"`
	ConfigurationDeterminate bool     `json:"configurationDeterminate"`
	ConfigurationReason      string   `json:"configurationReason,omitempty"`
	VersionReason            string   `json:"versionReason,omitempty"`
}

type Client struct {
	kube       kubernetes.Interface
	restConfig *rest.Config

	coreFactory     informers.SharedInformerFactory
	volcanoFactory  dynamicinformer.DynamicSharedInformerFactory
	podLister       corelisters.PodLister
	podIndexer      cache.Indexer
	nodeLister      corelisters.NodeLister
	configMapLister corelisters.ConfigMapLister
	priorityLister  schedulinglisters.PriorityClassLister
	podGroupLister  cache.GenericLister
	podGroupIndexer cache.Indexer
	queueLister     cache.GenericLister
	synced          []cache.InformerSynced

	startOnce sync.Once
	startErr  error
	cacheMu   sync.Mutex
}

func NewInClusterClient() (*Client, error) {
	config, err := loadRESTConfig()
	if err != nil {
		return nil, err
	}

	return NewClient(config)
}

func loadRESTConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	if paths := os.Getenv(clientcmd.RecommendedConfigPathEnvVar); paths != "" {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRules,
			&clientcmd.ConfigOverrides{},
		)

		config, fallbackErr := clientConfig.ClientConfig()
		if fallbackErr != nil {
			return nil, fmt.Errorf("load KUBECONFIG %q: %w", paths, fallbackErr)
		}

		return config, nil
	}

	return nil, fmt.Errorf("load in-cluster Kubernetes config: %w", err)
}

func NewClient(config *rest.Config) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("Kubernetes REST config is nil")
	}

	restConfig := rest.CopyConfig(config)
	restConfig.UserAgent = "volens"

	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}

	volcano, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}

	return newClientWithDynamic(kube, volcano, restConfig)
}

func newClient(kube kubernetes.Interface, restConfig *rest.Config) (*Client, error) {
	return newClientWithDynamic(kube, nil, restConfig)
}

func newClientWithDynamic(
	kube kubernetes.Interface,
	volcano dynamic.Interface,
	restConfig *rest.Config,
) (*Client, error) {
	if kube == nil {
		return nil, fmt.Errorf("Kubernetes client is nil")
	}

	coreFactory := informers.NewSharedInformerFactory(kube, 0)

	podInformer := coreFactory.Core().V1().Pods()
	nodeInformer := coreFactory.Core().V1().Nodes()
	configMapInformer := coreFactory.Core().V1().ConfigMaps()
	priorityInformer := coreFactory.Scheduling().V1().PriorityClasses()

	if err := podInformer.Informer().AddIndexers(cache.Indexers{
		podPhaseIndex: func(object any) ([]string, error) {
			pod, ok := object.(*corev1.Pod)
			if !ok {
				return nil, fmt.Errorf("pod phase index received %T", object)
			}

			return []string{string(pod.Status.Phase)}, nil
		},
		podGroupIndex:     podGroupIndexKeys,
		schedulerPodIndex: schedulerPodIndexKeys,
	}); err != nil {
		return nil, fmt.Errorf("add Pod informer index: %w", err)
	}

	manager := &Client{
		kube:            kube,
		restConfig:      restConfig,
		coreFactory:     coreFactory,
		podLister:       podInformer.Lister(),
		podIndexer:      podInformer.Informer().GetIndexer(),
		nodeLister:      nodeInformer.Lister(),
		configMapLister: configMapInformer.Lister(),
		priorityLister:  priorityInformer.Lister(),
		synced: []cache.InformerSynced{
			podInformer.Informer().HasSynced,
			nodeInformer.Informer().HasSynced,
			configMapInformer.Informer().HasSynced,
			priorityInformer.Informer().HasSynced,
		},
	}

	if volcano != nil {
		volcanoFactory := dynamicinformer.NewDynamicSharedInformerFactory(volcano, 0)
		podGroupInformer := volcanoFactory.ForResource(podGroupGVR)
		queueInformer := volcanoFactory.ForResource(queueGVR)

		if err := podGroupInformer.Informer().AddIndexers(cache.Indexers{
			podGroupQueueIndex: podGroupQueueIndexKeys,
		}); err != nil {
			return nil, fmt.Errorf("add PodGroup informer index: %w", err)
		}

		manager.volcanoFactory = volcanoFactory
		manager.podGroupLister = podGroupInformer.Lister()
		manager.podGroupIndexer = podGroupInformer.Informer().GetIndexer()
		manager.queueLister = queueInformer.Lister()
		manager.synced = append(
			manager.synced,
			podGroupInformer.Informer().HasSynced,
			queueInformer.Informer().HasSynced,
		)
	}

	return manager, nil
}

func (m *Client) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("Kubernetes informer context is nil")
	}

	m.startOnce.Do(func() {
		m.coreFactory.Start(ctx.Done())

		if m.volcanoFactory != nil {
			m.volcanoFactory.Start(ctx.Done())
		}

		if !cache.WaitForCacheSync(ctx.Done(), m.synced...) {
			m.startErr = fmt.Errorf("Kubernetes informer cache sync interrupted")
		}
	})

	return m.startErr
}

func (m *Client) Shutdown() {
	m.coreFactory.Shutdown()

	if m.volcanoFactory != nil {
		m.volcanoFactory.Shutdown()
	}
}

func (m *Client) ListPendingPods(ctx context.Context) ([]PendingPod, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	objects, err := m.podIndexer.ByIndex(podPhaseIndex, string(corev1.PodPending))
	if err != nil {
		return nil, fmt.Errorf("list Pending pods from informer cache: %w", err)
	}

	result := make([]PendingPod, 0, len(objects))

	for _, object := range objects {
		pod, ok := object.(*corev1.Pod)
		if !ok {
			continue
		}

		result = append(result, PendingPod{
			Namespace: pod.Namespace,
			Name:      pod.Name,
			Scheduler: pod.Spec.SchedulerName,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Namespace+"/"+result[i].Name < result[j].Namespace+"/"+result[j].Name
	})

	return result, nil
}

func (m *Client) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	pod, err := m.podLister.Pods(namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("get pod %s/%s from informer cache: %w", namespace, name, err)
	}

	return pod.DeepCopy(), nil
}

func (m *Client) ListNodes(ctx context.Context) ([]corev1.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	nodes, err := m.nodeLister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("list nodes from informer cache: %w", err)
	}

	result := make([]corev1.Node, 0, len(nodes))

	for _, node := range nodes {
		result = append(result, *node.DeepCopy())
	}

	return result, nil
}

func (m *Client) GetNode(ctx context.Context, name string) (*corev1.Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	node, err := m.nodeLister.Get(name)
	if err != nil {
		return nil, fmt.Errorf("get node %s from informer cache: %w", name, err)
	}

	return node.DeepCopy(), nil
}

// Events are queried on demand with a Pod-specific field selector. A global
// Event informer would retain a high-volume, short-lived resource stream.
func (m *Client) ListPodEvents(ctx context.Context, namespace, podName string) ([]corev1.Event, error) {
	pod, err := m.GetPod(ctx, namespace, podName)
	if err != nil {
		return nil, err
	}

	selectors := []fields.Selector{
		fields.OneTermEqualSelector("involvedObject.kind", "Pod"),
		fields.OneTermEqualSelector("involvedObject.name", podName),
	}

	if pod.UID != "" {
		selectors = append(
			selectors,
			fields.OneTermEqualSelector("involvedObject.uid", string(pod.UID)),
		)
	}

	events, err := m.kube.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fields.AndSelectors(selectors...).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list events for pod %s/%s: %w", namespace, podName, err)
	}

	result := make([]corev1.Event, 0, len(events.Items))

	for i := range events.Items {
		event := events.Items[i]

		if event.InvolvedObject.Kind == "Pod" &&
			event.InvolvedObject.Name == podName &&
			(pod.UID == "" || event.InvolvedObject.UID == pod.UID) {
			result = append(result, event)
		}
	}

	return result, nil
}

// ListPodGroupEvents reads scheduler events attached to the selected
// PodGroup. Volcano enqueue plugins commonly report rejection reasons on the
// PodGroup rather than on an individual Pod.
func (m *Client) ListPodGroupEvents(
	ctx context.Context,
	namespace string,
	podGroupName string,
	podGroupUID string,
) ([]corev1.Event, error) {
	if podGroupName == "" {
		return nil, nil
	}

	if podGroupUID == "" {
		return nil, fmt.Errorf("PodGroup %s/%s UID is required for Event lookup", namespace, podGroupName)
	}

	selectors := fields.AndSelectors(
		fields.OneTermEqualSelector("involvedObject.kind", "PodGroup"),
		fields.OneTermEqualSelector("involvedObject.name", podGroupName),
		fields.OneTermEqualSelector("involvedObject.uid", podGroupUID),
	)

	events, err := m.kube.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: selectors.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list events for PodGroup %s/%s: %w", namespace, podGroupName, err)
	}

	result := make([]corev1.Event, 0, len(events.Items))

	for i := range events.Items {
		event := events.Items[i]

		if event.InvolvedObject.Kind == "PodGroup" &&
			event.InvolvedObject.Name == podGroupName &&
			string(event.InvolvedObject.UID) == podGroupUID {
			result = append(result, event)
		}
	}

	return result, nil
}

func (m *Client) GetVolcanoScheduler(ctx context.Context) (Scheduler, error) {
	if err := ctx.Err(); err != nil {
		return Scheduler{}, err
	}

	objects, err := m.podIndexer.ByIndex(schedulerPodIndex, "true")
	if err != nil {
		return Scheduler{}, fmt.Errorf("list Volcano scheduler pods from informer index: %w", err)
	}

	identified := make([]*corev1.Pod, 0, len(objects))

	for _, object := range objects {
		pod, ok := object.(*corev1.Pod)
		if ok {
			identified = append(identified, pod)
		}
	}

	sort.Slice(identified, func(i, j int) bool {
		return identified[i].Namespace+"/"+identified[i].Name <
			identified[j].Namespace+"/"+identified[j].Name
	})

	candidates := readySchedulerPods(identified)

	if len(candidates) == 0 {
		if len(identified) == 0 {
			return Scheduler{}, ErrVolcanoSchedulerNotDiscovered
		}

		return Scheduler{}, ErrVolcanoSchedulerNotReady
	}

	chosen := candidates[0]

	if len(identified) > 1 {
		chosen, err = m.selectSchedulerLeader(ctx, identified, candidates)
		if err != nil {
			return Scheduler{}, err
		}
	}

	container := schedulerContainer(chosen)
	if container == nil {
		return Scheduler{}, fmt.Errorf("scheduler pod %s/%s has no containers", chosen.Namespace, chosen.Name)
	}

	schedulerNames, defaultQueue, configurationDeterminate, configurationReason :=
		schedulerRuntimeConfiguration(container)

	return Scheduler{
		Namespace:                chosen.Namespace,
		Name:                     chosen.Name,
		UID:                      string(chosen.UID),
		Container:                container.Name,
		Binary:                   schedulerBinary(container),
		Image:                    container.Image,
		Tag:                      imageTag(container.Image),
		SchedulerNames:           schedulerNames,
		DefaultQueue:             defaultQueue,
		ConfigurationDeterminate: configurationDeterminate,
		ConfigurationReason:      configurationReason,
	}, nil
}

func readySchedulerPods(pods []*corev1.Pod) []*corev1.Pod {
	result := make([]*corev1.Pod, 0, len(pods))

	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !isPodReady(pod) {
			continue
		}

		result = append(result, pod)
	}

	return result
}

func schedulerPodIndexKeys(object any) ([]string, error) {
	pod, ok := object.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("scheduler Pod index received %T", object)
	}

	if !isVolcanoSchedulerPod(pod) {
		return nil, nil
	}

	return []string{"true"}, nil
}

func isVolcanoSchedulerPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	for _, value := range []string{
		pod.Labels["app"],
		pod.Labels["app.kubernetes.io/name"],
		pod.Labels["component"],
	} {
		if exactSchedulerIdentity(value) {
			return true
		}
	}

	for index := range pod.Spec.Containers {
		container := &pod.Spec.Containers[index]

		if exactSchedulerIdentity(container.Name) ||
			exactSchedulerIdentity(imageRepositoryName(container.Image)) ||
			commandRunsScheduler(container.Command) {
			return true
		}
	}

	return false
}

func exactSchedulerIdentity(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))

	return value == "volcano-scheduler" || value == "vc-scheduler"
}

func imageRepositoryName(image string) string {
	withoutDigest, _, _ := strings.Cut(image, "@")
	base := filepath.Base(withoutDigest)
	name, _, _ := strings.Cut(base, ":")

	return name
}

func commandRunsScheduler(command []string) bool {
	for _, token := range command {
		if exactSchedulerIdentity(filepath.Base(token)) {
			return true
		}
	}

	return false
}

type schedulerLeaseKey struct {
	namespace string
	name      string
}

func (m *Client) selectSchedulerLeader(
	ctx context.Context,
	identified []*corev1.Pod,
	ready []*corev1.Pod,
) (*corev1.Pod, error) {
	keys := map[schedulerLeaseKey]struct{}{}
	leaderElectionEnabled := false
	leaderElectionDisabled := false

	for _, pod := range identified {
		container := schedulerContainer(pod)
		if container == nil {
			return nil, fmt.Errorf(
				"identify scheduler container in pod %s/%s before leader selection",
				pod.Namespace,
				pod.Name,
			)
		}

		options := parseSchedulerRuntimeOptions(container)
		if !options.determinate {
			return nil, fmt.Errorf(
				"resolve leader Lease for scheduler pod %s/%s: %s",
				pod.Namespace,
				pod.Name,
				options.reason,
			)
		}

		if !options.leaderElection {
			leaderElectionDisabled = true

			continue
		}

		leaderElectionEnabled = true
		keys[schedulerLeaseKey{
			namespace: options.lockObjectNamespace,
			name:      schedulerComponentName(options.schedulerNames),
		}] = struct{}{}
	}

	if leaderElectionEnabled && leaderElectionDisabled {
		return nil, fmt.Errorf("identified scheduler pods disagree on --leader-elect")
	}

	if !leaderElectionEnabled {
		if len(ready) == 1 {
			return ready[0], nil
		}

		return nil, fmt.Errorf("multiple ready scheduler pods have leader election disabled")
	}

	orderedKeys := make([]schedulerLeaseKey, 0, len(keys))

	for key := range keys {
		orderedKeys = append(orderedKeys, key)
	}

	sort.Slice(orderedKeys, func(i, j int) bool {
		return orderedKeys[i].namespace+"/"+orderedKeys[i].name <
			orderedKeys[j].namespace+"/"+orderedKeys[j].name
	})

	leases := make([]*coordinationv1.Lease, 0, len(orderedKeys))

	for _, key := range orderedKeys {
		lease, err := m.kube.CoordinationV1().Leases(key.namespace).Get(
			ctx,
			key.name,
			metav1.GetOptions{},
		)
		if apierrors.IsNotFound(err) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf(
				"get scheduler leader Lease %s/%s: %w",
				key.namespace,
				key.name,
				err,
			)
		}

		leases = append(leases, lease)
	}

	leader, err := schedulerLeaderFromLeases(identified, leases)
	if err != nil {
		return nil, err
	}

	for _, pod := range ready {
		if pod.Namespace == leader.Namespace && pod.Name == leader.Name && pod.UID == leader.UID {
			return leader, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: current Lease holder is %s/%s",
		ErrVolcanoSchedulerNotReady,
		leader.Namespace,
		leader.Name,
	)
}

func schedulerComponentName(schedulerNames []string) string {
	if len(schedulerNames) == 1 {
		return schedulerNames[0]
	}

	return defaultVolcanoSchedulerName
}

func schedulerLeaderFromLeases(
	candidates []*corev1.Pod,
	leases []*coordinationv1.Lease,
) (*corev1.Pod, error) {
	matches := map[string]*corev1.Pod{}

	for _, lease := range leases {
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
			continue
		}

		holder := *lease.Spec.HolderIdentity

		for _, pod := range candidates {
			if holder != pod.Name && !strings.HasPrefix(holder, pod.Name+"_") {
				continue
			}

			matches[pod.Namespace+"/"+pod.Name] = pod
		}
	}

	if len(matches) == 1 {
		for _, pod := range matches {
			return pod, nil
		}
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("no scheduler leader Lease holder matches an identified scheduler pod")
	}

	names := make([]string, 0, len(matches))

	for name := range matches {
		names = append(names, name)
	}

	sort.Strings(names)

	return nil, fmt.Errorf("scheduler leader is ambiguous across Lease holders: %s", strings.Join(names, ", "))
}

func schedulerContainer(pod *corev1.Pod) *corev1.Container {
	if pod == nil || len(pod.Spec.Containers) == 0 {
		return nil
	}

	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]

		if exactSchedulerIdentity(container.Name) ||
			exactSchedulerIdentity(imageRepositoryName(container.Image)) ||
			commandRunsScheduler(container.Command) {
			return container
		}
	}

	if len(pod.Spec.Containers) == 1 {
		return &pod.Spec.Containers[0]
	}

	return nil
}

func schedulerBinary(container *corev1.Container) string {
	if container == nil {
		return ""
	}

	tokens := append([]string(nil), container.Command...)
	tokens = append(tokens, container.Args...)

	for _, token := range tokens {
		if exactSchedulerIdentity(filepath.Base(token)) {
			return token
		}
	}

	return "vc-scheduler"
}

type schedulerRuntimeOptions struct {
	schedulerNames      []string
	defaultQueue        string
	leaderElection      bool
	lockObjectNamespace string
	determinate         bool
	reason              string
}

func schedulerRuntimeConfiguration(
	container *corev1.Container,
) ([]string, string, bool, string) {
	options := parseSchedulerRuntimeOptions(container)

	return options.schedulerNames,
		options.defaultQueue,
		options.determinate,
		options.reason
}

func parseSchedulerRuntimeOptions(container *corev1.Container) schedulerRuntimeOptions {
	if container == nil {
		return schedulerRuntimeOptions{reason: "scheduler container is unavailable"}
	}

	if shellWrapped(container.Command, container.Args) {
		return schedulerRuntimeOptions{
			reason: "scheduler command uses a shell wrapper, so runtime flags cannot be parsed safely",
		}
	}

	tokens := append([]string(nil), container.Command...)
	tokens = append(tokens, container.Args...)
	options := schedulerRuntimeOptions{
		schedulerNames:      []string{defaultVolcanoSchedulerName},
		defaultQueue:        defaultVolcanoQueue,
		leaderElection:      true,
		lockObjectNamespace: defaultSchedulerLockNS,
	}
	explicitSchedulerNames := false

	for index := 0; index < len(tokens); index++ {
		token := tokens[index]

		if token == "--" {
			break
		}

		name, value, matched, consumed, err := schedulerFlag(tokens, index)
		if err != nil {
			options.reason = err.Error()

			return options
		}

		if !matched {
			continue
		}

		if strings.Contains(value, "$(") || strings.Contains(value, "${") {
			options.reason = fmt.Sprintf("scheduler flag --%s uses runtime variable expansion", name)

			return options
		}

		if consumed {
			index++
		}

		switch name {
		case "scheduler-name":
			if !explicitSchedulerNames {
				options.schedulerNames = nil
				explicitSchedulerNames = true
			}

			if !containsString(options.schedulerNames, value) {
				options.schedulerNames = append(options.schedulerNames, value)
			}
		case "default-queue":
			options.defaultQueue = value
		case "lock-object-namespace":
			options.lockObjectNamespace = value
		case "leader-elect":
			enabled, err := strconv.ParseBool(value)
			if err != nil {
				options.reason = fmt.Sprintf("scheduler flag --leader-elect has invalid value %q", value)

				return options
			}

			options.leaderElection = enabled
		}
	}

	if len(options.schedulerNames) == 0 || options.defaultQueue == "" ||
		(options.leaderElection && options.lockObjectNamespace == "") {
		options.reason = "scheduler runtime configuration contains an empty scheduler name, default queue, or leader lock namespace"

		return options
	}

	options.determinate = true
	options.reason = "used standard Volcano defaults with tokenized explicit flag overrides"

	return options
}

func schedulerFlag(
	tokens []string,
	index int,
) (string, string, bool, bool, error) {
	token := tokens[index]

	for _, name := range []string{
		"scheduler-name",
		"default-queue",
		"lock-object-namespace",
		"leader-elect",
	} {
		flag := "--" + name

		if token == flag {
			if name == "leader-elect" &&
				(index+1 >= len(tokens) || strings.HasPrefix(tokens[index+1], "--")) {
				return name, "true", true, false, nil
			}

			if index+1 >= len(tokens) || strings.HasPrefix(tokens[index+1], "--") {
				return "", "", false, false, fmt.Errorf("scheduler flag %s has no value", flag)
			}

			return name, tokens[index+1], true, true, nil
		}

		if strings.HasPrefix(token, flag+"=") {
			value := strings.TrimPrefix(token, flag+"=")
			if value == "" {
				return "", "", false, false, fmt.Errorf("scheduler flag %s has an empty value", flag)
			}

			return name, value, true, false, nil
		}
	}

	return "", "", false, false, nil
}

func shellWrapped(command, arguments []string) bool {
	if len(command) == 0 {
		return false
	}

	name := strings.ToLower(filepath.Base(command[0]))
	if name != "sh" && name != "ash" && name != "bash" && name != "zsh" {
		return false
	}

	for _, argument := range append(append([]string(nil), command[1:]...), arguments...) {
		if strings.HasPrefix(argument, "-") &&
			!strings.HasPrefix(argument, "--") &&
			strings.Contains(strings.TrimPrefix(argument, "-"), "c") {
			return true
		}
	}

	return false
}

func containsString(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}

	return false
}

func isPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

func imageTag(image string) string {
	base := image

	if i := strings.Index(base, "@"); i >= 0 {
		base = base[:i]
	}

	slash := strings.LastIndex(base, "/")
	colon := strings.LastIndex(base, ":")

	if colon > slash {
		return base[colon+1:]
	}

	return "latest"
}

// GetVolcanoSchedulerLogs returns a bounded tail from the current scheduler
// leader. It is intentionally narrower than an arbitrary Pod log API so the
// final LLM fallback can inspect scheduler evidence without escaping the
// selected analysis scope.
func (m *Client) GetVolcanoSchedulerLogs(ctx context.Context, tailLines int64) (string, error) {
	scheduler, err := m.GetVolcanoScheduler(ctx)
	if err != nil {
		return "", err
	}

	tailLines = boundedSchedulerLogTail(tailLines)

	stream, err := m.kube.CoreV1().Pods(scheduler.Namespace).GetLogs(
		scheduler.Name,
		&corev1.PodLogOptions{
			Container: scheduler.Container,
			TailLines: &tailLines,
		},
	).Stream(ctx)
	if err != nil {
		return "", fmt.Errorf(
			"read logs from scheduler %s/%s container %s: %w",
			scheduler.Namespace,
			scheduler.Name,
			scheduler.Container,
			err,
		)
	}

	defer stream.Close()

	content, err := io.ReadAll(io.LimitReader(stream, maximumSchedulerLogResponse+1))
	if err != nil {
		return "", fmt.Errorf("read scheduler log stream: %w", err)
	}

	if len(content) > maximumSchedulerLogResponse {
		return "", fmt.Errorf("scheduler log result exceeds %d bytes", maximumSchedulerLogResponse)
	}

	return string(content), nil
}

func boundedSchedulerLogTail(tailLines int64) int64 {
	if tailLines <= 0 {
		return defaultSchedulerLogTail
	}

	if tailLines > maximumSchedulerLogTail {
		return maximumSchedulerLogTail
	}

	return tailLines
}

// GetVolcanoSchedulerVersion executes vc-scheduler --version in the selected
// leader Pod and parses the structured build lines printed by the binary.
func (m *Client) GetVolcanoSchedulerVersion(ctx context.Context) (Scheduler, error) {
	scheduler, err := m.GetVolcanoScheduler(ctx)
	if err != nil {
		return Scheduler{}, err
	}

	return m.GetVolcanoSchedulerVersionFor(ctx, scheduler)
}

// GetVolcanoSchedulerVersionFor enriches an already discovered scheduler
// identity with binary version metadata. Exec failures are returned separately
// so callers can keep branch selection manual.
func (m *Client) GetVolcanoSchedulerVersionFor(
	ctx context.Context,
	scheduler Scheduler,
) (Scheduler, error) {
	candidates := schedulerVersionCommands(scheduler.Binary)
	var lastErr error

	for _, command := range candidates {
		output, err := m.execInPod(
			ctx,
			scheduler.Namespace,
			scheduler.Name,
			scheduler.Container,
			[]string{command, "--version"},
		)
		if err != nil {
			lastErr = fmt.Errorf("%s --version: %w: %s", command, err, strings.TrimSpace(output))

			continue
		}

		version, parseErr := parseSchedulerVersionOutput(output)
		if parseErr != nil {
			lastErr = fmt.Errorf("%s --version: %w", command, parseErr)

			continue
		}

		version.Namespace = scheduler.Namespace
		version.Name = scheduler.Name
		version.UID = scheduler.UID
		version.Container = scheduler.Container
		version.Binary = command
		version.Image = scheduler.Image
		version.Tag = scheduler.Tag
		version.SchedulerNames = append([]string(nil), scheduler.SchedulerNames...)
		version.DefaultQueue = scheduler.DefaultQueue
		version.ConfigurationDeterminate = scheduler.ConfigurationDeterminate
		version.ConfigurationReason = scheduler.ConfigurationReason

		return version, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no scheduler binary candidates were available")
	}

	scheduler.VersionReason = lastErr.Error()

	return scheduler, lastErr
}

func schedulerVersionCommands(binary string) []string {
	candidates := []string{}

	if strings.TrimSpace(binary) != "" {
		candidates = append(candidates, strings.TrimSpace(binary))
	}

	candidates = append(candidates, "vc-scheduler", "./vc-scheduler", "/vc-scheduler")

	seen := map[string]bool{}
	result := make([]string, 0, len(candidates))

	for _, candidate := range candidates {
		if candidate == "" || seen[candidate] {
			continue
		}

		seen[candidate] = true
		result = append(result, candidate)
	}

	return result
}

func parseSchedulerVersionOutput(output string) (Scheduler, error) {
	var scheduler Scheduler

	for _, line := range strings.Split(output, "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), ":")
		if !found {
			continue
		}

		value = strings.TrimSpace(value)

		switch strings.TrimSpace(key) {
		case "Version":
			scheduler.Version = value
		case "Git SHA":
			scheduler.GitSHA = value
		case "Built At":
			scheduler.BuiltAt = value
		case "Go Version":
			scheduler.GoVersion = value
		case "Go OS/Arch":
			scheduler.GoOSArch = value
		}
	}

	if scheduler.Version == "" {
		return Scheduler{}, fmt.Errorf("scheduler version output did not contain a Version line")
	}

	return scheduler, nil
}

func (m *Client) streamPodLogs(ctx context.Context, scheduler Scheduler) (io.ReadCloser, error) {
	tailLines := int64(0)
	stream, err := m.kube.CoreV1().Pods(scheduler.Namespace).GetLogs(scheduler.Name, &corev1.PodLogOptions{
		Container: scheduler.Container,
		Follow:    true,
		TailLines: &tailLines,
	}).Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"stream logs from scheduler %s/%s container %s: %w",
			scheduler.Namespace,
			scheduler.Name,
			scheduler.Container,
			err,
		)
	}

	return stream, nil
}

func (m *Client) signalCacheDump(ctx context.Context, scheduler Scheduler) error {
	output, err := m.execInPod(
		ctx,
		scheduler.Namespace,
		scheduler.Name,
		scheduler.Container,
		[]string{"kill", "-s", "USR2", "1"},
	)
	if err != nil {
		return fmt.Errorf("signal scheduler cache dump: %w: %s", err, strings.TrimSpace(output))
	}

	return nil
}

func (m *Client) execInPod(
	ctx context.Context,
	namespace string,
	pod string,
	container string,
	command []string,
) (string, error) {
	request := m.kube.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec")

	request.VersionedParams(&corev1.PodExecOptions{
		Container: container,
		Command:   command,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(m.restConfig, http.MethodPost, request.URL())
	if err != nil {
		return "", fmt.Errorf("create SPDY executor: %w", err)
	}

	var stdout, stderr bytes.Buffer

	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})
	output := strings.TrimSpace(stdout.String() + "\n" + stderr.String())

	if err != nil {
		return output, err
	}

	return output, nil
}
