package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/volcano-sh/volens/internal/agent/validate"
	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
)

const (
	getTargetPodToolName       = "k8s_get_target_pod"
	listTargetEventsToolName   = "k8s_list_target_pod_events"
	listPodGroupEventsToolName = "k8s_list_target_podgroup_events"
	getNodeToolName            = "k8s_get_node"
	getSchedulerLogsToolName   = "k8s_get_volcano_scheduler_logs"
	readVolcanoSourceToolName  = "source_read_volcano_scheduler_file"

	defaultEventLimit        = 20
	maximumEventLimit        = 50
	defaultSchedulerLogLines = 200
	maximumSchedulerLogLines = 1000
	maximumToolLogText       = 48 << 10
	maximumToolSourceText    = 48 << 10
	maxToolArguments         = 8 << 10
	maxToolResultSize        = 64 << 10
)

type KubernetesToolScope struct {
	Namespace   string
	Pod         string
	NodeNames   []string
	SourceRoot  string
	SourceFiles []string
}

type kubernetesToolSession struct {
	clusterManager *cluster.Client
	scope          KubernetesToolScope
	allowedNodes   map[string]struct{}
	allowedSources map[string]struct{}
	targetPod      *corev1.Pod
	targetPodErr   error
	podLoaded      bool
}

type toolResponse struct {
	OK    bool   `json:"ok"`
	Data  any    `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

type podSchedulingView struct {
	Namespace                 string                            `json:"namespace"`
	Name                      string                            `json:"name"`
	UID                       string                            `json:"uid,omitempty"`
	Labels                    map[string]string                 `json:"labels,omitempty"`
	Annotations               map[string]string                 `json:"annotations,omitempty"`
	Phase                     corev1.PodPhase                   `json:"phase"`
	SchedulerName             string                            `json:"schedulerName"`
	NodeName                  string                            `json:"nodeName,omitempty"`
	NominatedNodeName         string                            `json:"nominatedNodeName,omitempty"`
	Priority                  *int32                            `json:"priority,omitempty"`
	PriorityClassName         string                            `json:"priorityClassName,omitempty"`
	PreemptionPolicy          *corev1.PreemptionPolicy          `json:"preemptionPolicy,omitempty"`
	RuntimeClassName          *string                           `json:"runtimeClassName,omitempty"`
	HostNetwork               bool                              `json:"hostNetwork"`
	NodeSelector              map[string]string                 `json:"nodeSelector,omitempty"`
	Affinity                  *corev1.Affinity                  `json:"affinity,omitempty"`
	Tolerations               []corev1.Toleration               `json:"tolerations,omitempty"`
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	SchedulingGates           []corev1.PodSchedulingGate        `json:"schedulingGates,omitempty"`
	Overhead                  corev1.ResourceList               `json:"overhead,omitempty"`
	Containers                []containerSchedulingView         `json:"containers"`
	InitContainers            []containerSchedulingView         `json:"initContainers,omitempty"`
	PersistentVolumeClaims    []persistentVolumeClaimView       `json:"persistentVolumeClaims,omitempty"`
	Conditions                []podConditionView                `json:"conditions,omitempty"`
	ContainerStatuses         []containerStatusView             `json:"containerStatuses,omitempty"`
}

type containerSchedulingView struct {
	Name      string              `json:"name"`
	Requests  corev1.ResourceList `json:"requests,omitempty"`
	Limits    corev1.ResourceList `json:"limits,omitempty"`
	HostPorts []hostPortView      `json:"hostPorts,omitempty"`
}

type hostPortView struct {
	Protocol corev1.Protocol `json:"protocol"`
	HostIP   string          `json:"hostIP,omitempty"`
	HostPort int32           `json:"hostPort"`
}

type persistentVolumeClaimView struct {
	VolumeName string `json:"volumeName"`
	ClaimName  string `json:"claimName"`
	ReadOnly   bool   `json:"readOnly"`
}

type podConditionView struct {
	Type               corev1.PodConditionType `json:"type"`
	Status             corev1.ConditionStatus  `json:"status"`
	Reason             string                  `json:"reason,omitempty"`
	Message            string                  `json:"message,omitempty"`
	LastTransitionTime string                  `json:"lastTransitionTime,omitempty"`
}

type containerStatusView struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restartCount"`
	State        string `json:"state,omitempty"`
	Reason       string `json:"reason,omitempty"`
	Message      string `json:"message,omitempty"`
}

type podEventView struct {
	Type                string `json:"type,omitempty"`
	Reason              string `json:"reason,omitempty"`
	Message             string `json:"message,omitempty"`
	Count               int32  `json:"count,omitempty"`
	Action              string `json:"action,omitempty"`
	SourceComponent     string `json:"sourceComponent,omitempty"`
	ReportingController string `json:"reportingController,omitempty"`
	Timestamp           string `json:"timestamp,omitempty"`
}

type nodeSchedulingView struct {
	Name          string              `json:"name"`
	Unschedulable bool                `json:"unschedulable"`
	Labels        map[string]string   `json:"labels,omitempty"`
	Taints        []corev1.Taint      `json:"taints,omitempty"`
	Capacity      corev1.ResourceList `json:"capacity,omitempty"`
	Allocatable   corev1.ResourceList `json:"allocatable,omitempty"`
	Conditions    []nodeConditionView `json:"conditions,omitempty"`
}

type nodeConditionView struct {
	Type               corev1.NodeConditionType `json:"type"`
	Status             corev1.ConditionStatus   `json:"status"`
	Reason             string                   `json:"reason,omitempty"`
	LastTransitionTime string                   `json:"lastTransitionTime,omitempty"`
}

var readOnlyKubernetesTools = []chatTool{
	{
		Type: "function",
		Function: chatToolFunction{
			Name:        getTargetPodToolName,
			Description: "Return scheduling-relevant, sanitized details for the selected Pending Pod. The target is fixed by the analysis; this tool cannot read another Pod.",
			Parameters: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{},
				"additionalProperties": false,
			},
		},
	},
	{
		Type: "function",
		Function: chatToolFunction{
			Name:        listPodGroupEventsToolName,
			Description: "List recent Kubernetes Events attached to the selected Pod's PodGroup. The PodGroup is derived from the fixed target Pod and cannot be selected arbitrarily.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"limit": map[string]any{
						"type":        "integer",
						"description": "Maximum number of newest events to return.",
						"minimum":     1,
						"maximum":     maximumEventLimit,
					},
				},
				"additionalProperties": false,
			},
		},
	},
	{
		Type: "function",
		Function: chatToolFunction{
			Name:        listTargetEventsToolName,
			Description: "List recent Kubernetes Events for the selected Pod only. An empty or failed result is uncertainty, not proof of schedulability.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"limit": map[string]any{
						"type":        "integer",
						"description": "Maximum number of newest events to return.",
						"minimum":     1,
						"maximum":     maximumEventLimit,
					},
				},
				"additionalProperties": false,
			},
		},
	},
	{
		Type: "function",
		Function: chatToolFunction{
			Name:        getNodeToolName,
			Description: "Return scheduling-relevant, sanitized details for one node already included in this analysis.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{
						"type":        "string",
						"description": "Exact node name from the analyzed node results.",
					},
				},
				"required":             []string{"name"},
				"additionalProperties": false,
			},
		},
	},
	{
		Type: "function",
		Function: chatToolFunction{
			Name:        getSchedulerLogsToolName,
			Description: "Return a bounded log tail from the current Volcano scheduler leader. Use it only when report and event evidence do not establish the plugin outcome.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"tailLines": map[string]any{
						"type":        "integer",
						"description": "Number of scheduler log lines to request.",
						"minimum":     1,
						"maximum":     maximumSchedulerLogLines,
					},
				},
				"additionalProperties": false,
			},
		},
	},
	{
		Type: "function",
		Function: chatToolFunction{
			Name:        readVolcanoSourceToolName,
			Description: "Read one dynamically discovered Volcano scheduler hook source file from the exact selected branch or tag worktree. Only files listed in the source index are allowed.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{
						"type":        "string",
						"description": "Repository-relative .go path from the supplied source index.",
					},
				},
				"required":             []string{"path"},
				"additionalProperties": false,
			},
		},
	},
}

func (c LLMConfig) CompleteWithTools(
	ctx context.Context,
	prompt string,
	clusterManager *cluster.Client,
	scope KubernetesToolScope,
) (string, error) {
	session, err := newKubernetesToolSession(clusterManager, scope)
	if err != nil {
		return "", err
	}

	return c.complete(ctx, prompt, readOnlyKubernetesTools, session.execute)
}

func newKubernetesToolSession(
	clusterManager *cluster.Client,
	scope KubernetesToolScope,
) (*kubernetesToolSession, error) {
	if clusterManager == nil {
		return nil, fmt.Errorf("Kubernetes cluster manager is nil")
	}

	if scope.Namespace == "" || scope.Pod == "" {
		return nil, fmt.Errorf("Kubernetes tool scope requires namespace and pod")
	}

	allowedNodes := make(map[string]struct{}, len(scope.NodeNames))
	for _, name := range scope.NodeNames {
		if name != "" {
			allowedNodes[name] = struct{}{}
		}
	}

	allowedSources := make(map[string]struct{}, len(scope.SourceFiles))

	for _, path := range scope.SourceFiles {
		clean := filepath.ToSlash(filepath.Clean(path))

		if clean != "." && !filepath.IsAbs(path) && !strings.HasPrefix(clean, "../") {
			allowedSources[clean] = struct{}{}
		}
	}

	return &kubernetesToolSession{
		clusterManager: clusterManager,
		scope:          scope,
		allowedNodes:   allowedNodes,
		allowedSources: allowedSources,
	}, nil
}

func (s *kubernetesToolSession) execute(ctx context.Context, call chatToolCall) string {
	if len(call.Function.Arguments) > maxToolArguments {
		return encodeToolError("tool arguments exceed the size limit")
	}

	switch call.Function.Name {
	case getTargetPodToolName:
		var arguments struct{}
		if err := decodeToolArguments(call.Function.Arguments, &arguments); err != nil {
			return encodeToolError(err.Error())
		}

		pod, err := s.getTargetPod(ctx)
		if err != nil {
			return encodeToolError(err.Error())
		}

		return encodeToolSuccess(summarizePod(pod))

	case listTargetEventsToolName:
		arguments := struct {
			Limit int `json:"limit"`
		}{
			Limit: defaultEventLimit,
		}
		if err := decodeToolArguments(call.Function.Arguments, &arguments); err != nil {
			return encodeToolError(err.Error())
		}

		if arguments.Limit < 1 || arguments.Limit > maximumEventLimit {
			return encodeToolError(fmt.Sprintf("limit must be between 1 and %d", maximumEventLimit))
		}

		events, err := s.clusterManager.ListPodEvents(ctx, s.scope.Namespace, s.scope.Pod)
		if err != nil {
			return encodeToolError(err.Error())
		}

		return encodeToolSuccess(summarizeEvents(events, arguments.Limit))

	case listPodGroupEventsToolName:
		arguments := struct {
			Limit int `json:"limit"`
		}{
			Limit: defaultEventLimit,
		}
		if err := decodeToolArguments(call.Function.Arguments, &arguments); err != nil {
			return encodeToolError(err.Error())
		}

		if arguments.Limit < 1 || arguments.Limit > maximumEventLimit {
			return encodeToolError(fmt.Sprintf("limit must be between 1 and %d", maximumEventLimit))
		}

		pod, err := s.getTargetPod(ctx)
		if err != nil {
			return encodeToolError(err.Error())
		}

		podGroup := validate.PodGroupName(pod)
		if podGroup == "" {
			return encodeToolError("target Pod has no PodGroup association")
		}

		group, err := s.clusterManager.GetPodGroup(ctx, s.scope.Namespace, podGroup)
		if err != nil {
			return encodeToolError(err.Error())
		}

		events, err := s.clusterManager.ListPodGroupEvents(
			ctx,
			s.scope.Namespace,
			podGroup,
			group.UID,
		)
		if err != nil {
			return encodeToolError(err.Error())
		}

		return encodeToolSuccess(summarizeEvents(events, arguments.Limit))

	case getNodeToolName:
		var arguments struct {
			Name string `json:"name"`
		}
		if err := decodeToolArguments(call.Function.Arguments, &arguments); err != nil {
			return encodeToolError(err.Error())
		}

		if _, allowed := s.allowedNodes[arguments.Name]; !allowed {
			return encodeToolError(fmt.Sprintf("node %q is outside this analysis", arguments.Name))
		}

		node, err := s.clusterManager.GetNode(ctx, arguments.Name)
		if err != nil {
			return encodeToolError(err.Error())
		}

		pod, err := s.getTargetPod(ctx)
		if err != nil {
			return encodeToolError(err.Error())
		}

		return encodeToolSuccess(summarizeNode(node, pod))

	case getSchedulerLogsToolName:
		arguments := struct {
			TailLines int64 `json:"tailLines"`
		}{
			TailLines: defaultSchedulerLogLines,
		}
		if err := decodeToolArguments(call.Function.Arguments, &arguments); err != nil {
			return encodeToolError(err.Error())
		}

		if arguments.TailLines < 1 || arguments.TailLines > maximumSchedulerLogLines {
			return encodeToolError(
				fmt.Sprintf("tailLines must be between 1 and %d", maximumSchedulerLogLines),
			)
		}

		logs, err := s.clusterManager.GetVolcanoSchedulerLogs(ctx, arguments.TailLines)
		if err != nil {
			return encodeToolError(err.Error())
		}

		return encodeToolSuccess(map[string]any{
			"tailLines": arguments.TailLines,
			"logs":      limitToolText(logs, maximumToolLogText),
		})

	case readVolcanoSourceToolName:
		var arguments struct {
			Path string `json:"path"`
		}
		if err := decodeToolArguments(call.Function.Arguments, &arguments); err != nil {
			return encodeToolError(err.Error())
		}

		content, err := readScopedSourceFile(
			s.scope.SourceRoot,
			arguments.Path,
			s.allowedSources,
		)
		if err != nil {
			return encodeToolError(err.Error())
		}

		return encodeToolSuccess(map[string]any{
			"path":    arguments.Path,
			"content": limitToolText(content, maximumToolSourceText),
		})

	default:
		return encodeToolError(fmt.Sprintf("tool %q is not allowed", call.Function.Name))
	}
}

func readScopedSourceFile(
	root string,
	requested string,
	allowed map[string]struct{},
) (string, error) {
	clean := filepath.ToSlash(filepath.Clean(requested))

	if root == "" || clean == "." || filepath.IsAbs(requested) || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("source path %q is outside the selected worktree", requested)
	}

	if _, found := allowed[clean]; !found {
		return "", fmt.Errorf("source path %q is not in the selected hook index", requested)
	}

	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve selected worktree: %w", err)
	}

	resolvedPath, err := filepath.EvalSymlinks(filepath.Join(resolvedRoot, filepath.FromSlash(clean)))
	if err != nil {
		return "", fmt.Errorf("resolve source path %q: %w", requested, err)
	}

	relative, err := filepath.Rel(resolvedRoot, resolvedPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("source path %q escapes the selected worktree", requested)
	}

	file, err := os.Open(resolvedPath)
	if err != nil {
		return "", fmt.Errorf("open source path %q: %w", requested, err)
	}

	defer file.Close()

	content, err := io.ReadAll(io.LimitReader(file, maximumToolSourceText+1))
	if err != nil {
		return "", fmt.Errorf("read source path %q: %w", requested, err)
	}

	if len(content) > maximumToolSourceText {
		return string(content[:maximumToolSourceText]) + "...", nil
	}

	return string(content), nil
}

func (s *kubernetesToolSession) getTargetPod(ctx context.Context) (*corev1.Pod, error) {
	if !s.podLoaded {
		s.targetPod, s.targetPodErr = s.clusterManager.GetPod(ctx, s.scope.Namespace, s.scope.Pod)
		s.podLoaded = true
	}

	return s.targetPod, s.targetPodErr
}

func decodeToolArguments(raw string, destination any) error {
	if strings.TrimSpace(raw) == "" {
		raw = "{}"
	}

	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid tool arguments: %w", err)
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("invalid tool arguments: expected one JSON object")
		}

		return fmt.Errorf("invalid tool arguments: %w", err)
	}

	return nil
}

func encodeToolSuccess(data any) string {
	encoded, err := json.Marshal(toolResponse{OK: true, Data: data})
	if err != nil {
		return encodeToolError("encode tool result: " + err.Error())
	}

	if len(encoded) > maxToolResultSize {
		return encodeToolError("tool result exceeds the size limit")
	}

	return string(encoded)
}

func encodeToolError(message string) string {
	if len(message) > 2048 {
		message = message[:2048]
	}

	encoded, err := json.Marshal(toolResponse{OK: false, Error: message})
	if err != nil {
		return `{"ok":false,"error":"encode tool error failed"}`
	}

	return string(encoded)
}

func summarizePod(pod *corev1.Pod) podSchedulingView {
	view := podSchedulingView{
		Namespace:                 pod.Namespace,
		Name:                      pod.Name,
		UID:                       string(pod.UID),
		Labels:                    schedulingMetadata(pod.Labels),
		Annotations:               schedulingMetadata(pod.Annotations),
		Phase:                     pod.Status.Phase,
		SchedulerName:             pod.Spec.SchedulerName,
		NodeName:                  pod.Spec.NodeName,
		NominatedNodeName:         pod.Status.NominatedNodeName,
		Priority:                  pod.Spec.Priority,
		PriorityClassName:         pod.Spec.PriorityClassName,
		PreemptionPolicy:          pod.Spec.PreemptionPolicy,
		RuntimeClassName:          pod.Spec.RuntimeClassName,
		HostNetwork:               pod.Spec.HostNetwork,
		NodeSelector:              cloneStringMap(pod.Spec.NodeSelector),
		Affinity:                  pod.Spec.Affinity.DeepCopy(),
		Tolerations:               append([]corev1.Toleration(nil), pod.Spec.Tolerations...),
		TopologySpreadConstraints: append([]corev1.TopologySpreadConstraint(nil), pod.Spec.TopologySpreadConstraints...),
		SchedulingGates:           append([]corev1.PodSchedulingGate(nil), pod.Spec.SchedulingGates...),
		Overhead:                  pod.Spec.Overhead.DeepCopy(),
		Containers:                summarizeContainers(pod.Spec.Containers),
		InitContainers:            summarizeContainers(pod.Spec.InitContainers),
	}

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		view.PersistentVolumeClaims = append(view.PersistentVolumeClaims, persistentVolumeClaimView{
			VolumeName: volume.Name,
			ClaimName:  volume.PersistentVolumeClaim.ClaimName,
			ReadOnly:   volume.PersistentVolumeClaim.ReadOnly,
		})
	}

	for _, condition := range pod.Status.Conditions {
		view.Conditions = append(view.Conditions, podConditionView{
			Type:               condition.Type,
			Status:             condition.Status,
			Reason:             condition.Reason,
			Message:            limitToolText(condition.Message, 2048),
			LastTransitionTime: formatTime(condition.LastTransitionTime.Time),
		})
	}

	for _, status := range pod.Status.ContainerStatuses {
		state, reason, message := summarizeContainerState(status.State)
		view.ContainerStatuses = append(view.ContainerStatuses, containerStatusView{
			Name:         status.Name,
			Ready:        status.Ready,
			RestartCount: status.RestartCount,
			State:        state,
			Reason:       reason,
			Message:      limitToolText(message, 2048),
		})
	}

	return view
}

func summarizeContainers(containers []corev1.Container) []containerSchedulingView {
	result := make([]containerSchedulingView, 0, len(containers))

	for _, container := range containers {
		view := containerSchedulingView{
			Name:     container.Name,
			Requests: container.Resources.Requests.DeepCopy(),
			Limits:   container.Resources.Limits.DeepCopy(),
		}

		for _, port := range container.Ports {
			if port.HostPort == 0 {
				continue
			}

			view.HostPorts = append(view.HostPorts, hostPortView{
				Protocol: port.Protocol,
				HostIP:   port.HostIP,
				HostPort: port.HostPort,
			})
		}

		result = append(result, view)
	}

	return result
}

func summarizeContainerState(state corev1.ContainerState) (string, string, string) {
	if state.Waiting != nil {
		return "waiting", state.Waiting.Reason, state.Waiting.Message
	}

	if state.Running != nil {
		return "running", "", ""
	}

	if state.Terminated != nil {
		return "terminated", state.Terminated.Reason, state.Terminated.Message
	}

	return "", "", ""
}

func summarizeEvents(events []corev1.Event, limit int) []podEventView {
	items := append([]corev1.Event(nil), events...)
	sort.SliceStable(items, func(i, j int) bool {
		return eventTime(items[i]).After(eventTime(items[j]))
	})

	if len(items) > limit {
		items = items[:limit]
	}

	result := make([]podEventView, 0, len(items))
	for _, event := range items {
		result = append(result, podEventView{
			Type:                event.Type,
			Reason:              event.Reason,
			Message:             limitToolText(event.Message, 4096),
			Count:               event.Count,
			Action:              event.Action,
			SourceComponent:     event.Source.Component,
			ReportingController: event.ReportingController,
			Timestamp:           formatTime(eventTime(event)),
		})
	}

	return result
}

func eventTime(event corev1.Event) time.Time {
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}

	if event.Series != nil && !event.Series.LastObservedTime.IsZero() {
		return event.Series.LastObservedTime.Time
	}

	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}

	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}

	return event.CreationTimestamp.Time
}

func summarizeNode(node *corev1.Node, pod *corev1.Pod) nodeSchedulingView {
	return nodeSchedulingView{
		Name:          node.Name,
		Unschedulable: node.Spec.Unschedulable,
		Labels:        schedulingNodeLabels(node.Labels, pod),
		Taints:        append([]corev1.Taint(nil), node.Spec.Taints...),
		Capacity:      node.Status.Capacity.DeepCopy(),
		Allocatable:   node.Status.Allocatable.DeepCopy(),
		Conditions:    summarizeNodeConditions(node.Status.Conditions),
	}
}

func summarizeNodeConditions(conditions []corev1.NodeCondition) []nodeConditionView {
	result := make([]nodeConditionView, 0, len(conditions))

	for _, condition := range conditions {
		result = append(result, nodeConditionView{
			Type:               condition.Type,
			Status:             condition.Status,
			Reason:             condition.Reason,
			LastTransitionTime: formatTime(condition.LastTransitionTime.Time),
		})
	}

	return result
}

func schedulingMetadata(values map[string]string) map[string]string {
	result := map[string]string{}

	for key, value := range values {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "volcano") || strings.Contains(lower, "scheduling") {
			result[key] = value
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

func schedulingNodeLabels(labels map[string]string, pod *corev1.Pod) map[string]string {
	keys := map[string]struct{}{}

	for key := range pod.Spec.NodeSelector {
		keys[key] = struct{}{}
	}

	if affinity := pod.Spec.Affinity; affinity != nil {
		if nodeAffinity := affinity.NodeAffinity; nodeAffinity != nil {
			if required := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution; required != nil {
				for _, term := range required.NodeSelectorTerms {
					for _, expression := range term.MatchExpressions {
						keys[expression.Key] = struct{}{}
					}
				}
			}

			for _, preferred := range nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				for _, expression := range preferred.Preference.MatchExpressions {
					keys[expression.Key] = struct{}{}
				}
			}
		}

		for _, term := range podAffinityTerms(affinity.PodAffinity) {
			keys[term.TopologyKey] = struct{}{}
		}

		for _, term := range podAntiAffinityTerms(affinity.PodAntiAffinity) {
			keys[term.TopologyKey] = struct{}{}
		}
	}

	for _, constraint := range pod.Spec.TopologySpreadConstraints {
		keys[constraint.TopologyKey] = struct{}{}
	}

	result := map[string]string{}
	for key, value := range labels {
		_, selected := keys[key]
		standard := strings.HasPrefix(key, "kubernetes.io/") ||
			strings.HasPrefix(key, "node.kubernetes.io/") ||
			strings.HasPrefix(key, "topology.kubernetes.io/")

		if selected || standard {
			result[key] = value
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

func podAffinityTerms(affinity *corev1.PodAffinity) []corev1.PodAffinityTerm {
	if affinity == nil {
		return nil
	}

	result := append([]corev1.PodAffinityTerm(nil), affinity.RequiredDuringSchedulingIgnoredDuringExecution...)
	for _, preferred := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
		result = append(result, preferred.PodAffinityTerm)
	}

	return result
}

func podAntiAffinityTerms(affinity *corev1.PodAntiAffinity) []corev1.PodAffinityTerm {
	if affinity == nil {
		return nil
	}

	result := append([]corev1.PodAffinityTerm(nil), affinity.RequiredDuringSchedulingIgnoredDuringExecution...)
	for _, preferred := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
		result = append(result, preferred.PodAffinityTerm)
	}

	return result
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}

	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}

	return result
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}

	return value.UTC().Format(time.RFC3339Nano)
}

func limitToolText(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}

	return value[:maximum] + "..."
}
