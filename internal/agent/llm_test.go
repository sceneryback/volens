package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCompleteWithoutToolsUsesSingleTextCompletion(t *testing.T) {
	var requests atomic.Int32

	config := llmTestConfig(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)

		var body chatCompletionRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}

		if len(body.Tools) != 0 || body.ToolChoice != "" {
			t.Errorf("unexpected tools=%v toolChoice=%q", body.Tools, body.ToolChoice)
		}

		if authorization := request.Header.Get("Authorization"); authorization != "Bearer test-key" {
			t.Errorf("authorization=%q", authorization)
		}

		writeLLMResponse(writer, map[string]any{
			"role":    "assistant",
			"content": "plain answer",
		})
	}))
	config.Key = "test-key"

	answer, err := config.Complete(context.Background(), "diagnose")
	if err != nil {
		t.Fatal(err)
	}

	if answer != "plain answer" || requests.Load() != 1 {
		t.Fatalf("answer=%q requests=%d", answer, requests.Load())
	}
}

func TestCompleteToolProtocolPreservesCallsAndResults(t *testing.T) {
	var requests atomic.Int32
	var executions atomic.Int32

	config := llmTestConfig(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		round := requests.Add(1)

		var body chatCompletionRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}

		if len(body.Tools) != len(readOnlyKubernetesTools) || body.ToolChoice != "auto" {
			t.Errorf("tools=%d toolChoice=%q", len(body.Tools), body.ToolChoice)
		}

		if round == 1 {
			writeLLMResponse(writer, map[string]any{
				"role":    "assistant",
				"content": nil,
				"tool_calls": []map[string]any{
					{
						"id":   "call-pod",
						"type": "function",
						"function": map[string]any{
							"name":      getTargetPodToolName,
							"arguments": `{}`,
						},
					},
					{
						"id":   "call-node",
						"type": "function",
						"function": map[string]any{
							"name":      getNodeToolName,
							"arguments": `{"name":"node-a"}`,
						},
					},
				},
			})

			return
		}

		if len(body.Messages) != 5 {
			t.Errorf("messages=%d, want 5", len(body.Messages))
		} else {
			assistant := body.Messages[2]
			if assistant.Role != "assistant" || assistant.Content != nil || len(assistant.ToolCalls) != 2 {
				t.Errorf("assistant message=%+v", assistant)
			}

			podResult := dereference(body.Messages[3].Content)
			nodeResult := dereference(body.Messages[4].Content)

			if body.Messages[3].ToolCallID != "call-pod" || body.Messages[4].ToolCallID != "call-node" {
				t.Errorf("tool call IDs=%q,%q", body.Messages[3].ToolCallID, body.Messages[4].ToolCallID)
			}

			if !strings.Contains(podResult, getTargetPodToolName) ||
				!strings.Contains(nodeResult, getNodeToolName) {
				t.Errorf("unexpected tool results pod=%s node=%s", podResult, nodeResult)
			}
		}

		writeLLMResponse(writer, map[string]any{
			"role":    "assistant",
			"content": "tool-supported answer",
		})
	}))

	execute := func(_ context.Context, call chatToolCall) string {
		executions.Add(1)

		return encodeToolSuccess(map[string]string{"tool": call.Function.Name})
	}

	answer, err := config.complete(
		context.Background(),
		"diagnose",
		readOnlyKubernetesTools,
		execute,
	)
	if err != nil {
		t.Fatal(err)
	}

	if answer != "tool-supported answer" || executions.Load() != 2 {
		t.Fatalf("answer=%q executions=%d", answer, executions.Load())
	}
}

func TestCompleteStopsAtToolRoundLimitAndCachesCalls(t *testing.T) {
	var requests atomic.Int32
	var executions atomic.Int32

	config := llmTestConfig(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		round := requests.Add(1)

		writeLLMResponse(writer, map[string]any{
			"role":    "assistant",
			"content": nil,
			"tool_calls": []map[string]any{
				{
					"id":   fmt.Sprintf("call-%d", round),
					"type": "function",
					"function": map[string]any{
						"name":      getTargetPodToolName,
						"arguments": `{}`,
					},
				},
			},
		})
	}))
	config.MaxToolRounds = 2

	_, err := config.complete(
		context.Background(),
		"diagnose",
		readOnlyKubernetesTools,
		func(context.Context, chatToolCall) string {
			executions.Add(1)

			return encodeToolSuccess(map[string]bool{"cached": true})
		},
	)
	if !errors.Is(err, ErrLLMToolRoundLimit) {
		t.Fatalf("err=%v", err)
	}

	var limitError *LLMToolRoundLimitError
	if !errors.As(err, &limitError) {
		t.Fatalf("error type=%T, want *LLMToolRoundLimitError", err)
	}

	if limitError.CompletedRounds != 2 || limitError.MaxRounds != 2 ||
		!strings.Contains(err.Error(), "2 completed rounds") {
		t.Fatalf("limit error=%+v message=%q", limitError, err)
	}

	if requests.Load() != int32(config.MaxToolRounds+1) {
		t.Fatalf("requests=%d", requests.Load())
	}

	if executions.Load() != 1 {
		t.Fatalf("identical tool call executed %d times", executions.Load())
	}
}

func TestLLMConfigFromEnvMaxToolRounds(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  int
	}{
		{
			name: "default",
			want: defaultMaxToolRounds,
		},
		{
			name:  "configured",
			value: "7",
			want:  7,
		},
		{
			name:  "zero falls back",
			value: "0",
			want:  defaultMaxToolRounds,
		},
		{
			name:  "negative falls back",
			value: "-1",
			want:  defaultMaxToolRounds,
		},
		{
			name:  "invalid falls back",
			value: "invalid",
			want:  defaultMaxToolRounds,
		},
		{
			name:  "bounded",
			value: "100",
			want:  maximumMaxToolRounds,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("LLM_MAX_TOOL_ROUNDS", test.value)

			config := LLMConfigFromEnv()
			if config.MaxToolRounds != test.want {
				t.Fatalf("MaxToolRounds=%d, want %d", config.MaxToolRounds, test.want)
			}
		})
	}
}

func TestLLMConfigFromEnvTimeout(t *testing.T) {
	tests := []struct {
		name          string
		maxToolRounds string
		timeout       string
		want          time.Duration
	}{
		{
			name: "default follows default rounds",
			want: defaultLLMCompletionTimeout(defaultMaxToolRounds),
		},
		{
			name:          "default follows configured rounds",
			maxToolRounds: "7",
			want:          defaultLLMCompletionTimeout(7),
		},
		{
			name:    "configured Go duration",
			timeout: "3m30s",
			want:    3*time.Minute + 30*time.Second,
		},
		{
			name:    "zero falls back",
			timeout: "0s",
			want:    defaultLLMCompletionTimeout(defaultMaxToolRounds),
		},
		{
			name:    "negative falls back",
			timeout: "-1m",
			want:    defaultLLMCompletionTimeout(defaultMaxToolRounds),
		},
		{
			name:    "invalid falls back",
			timeout: "invalid",
			want:    defaultLLMCompletionTimeout(defaultMaxToolRounds),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("LLM_MAX_TOOL_ROUNDS", test.maxToolRounds)
			t.Setenv("LLM_TIMEOUT", test.timeout)

			config := LLMConfigFromEnv()
			if config.Timeout != test.want {
				t.Fatalf("Timeout=%s, want %s", config.Timeout, test.want)
			}
		})
	}
}

func TestDefaultLLMTimeoutScalesWithEveryAllowedModelResponse(t *testing.T) {
	for maxToolRounds := 1; maxToolRounds <= maximumMaxToolRounds; maxToolRounds++ {
		modelResponses := maxToolRounds + 1
		want := time.Duration(modelResponses) * llmRoundTimeoutBudget
		got := defaultLLMCompletionTimeout(maxToolRounds)

		if got != want {
			t.Fatalf(
				"rounds=%d timeout=%s, want %s",
				maxToolRounds,
				got,
				want,
			)
		}
	}
}

func TestCompleteHonorsConfiguredTotalTimeout(t *testing.T) {
	config := LLMConfig{
		URL:     "http://llm.example/v1",
		Model:   "test-model",
		Timeout: 20 * time.Millisecond,
		httpClient: &http.Client{
			Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				<-request.Context().Done()

				return nil, request.Context().Err()
			}),
		},
	}

	_, err := config.Complete(context.Background(), "diagnose")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want context deadline exceeded", err)
	}
}

func TestLLMFallbackRecommendationForBoundedStops(t *testing.T) {
	config := LLMConfig{
		MaxToolRounds: 3,
		Timeout:       7 * time.Minute,
	}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "tool round limit",
			err: &LLMToolRoundLimitError{
				CompletedRounds: 3,
				MaxRounds:       3,
			},
			want: "maximum of 3 tool rounds",
		},
		{
			name: "deadline",
			err:  fmt.Errorf("send request: %w", context.DeadlineExceeded),
			want: "configured total timeout 7m0s",
		},
		{
			name: "canceled",
			err:  fmt.Errorf("send request: %w", context.Canceled),
			want: "was canceled",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, ok := llmFallbackRecommendation(test.err, config)
			if !ok {
				t.Fatal("expected a manual-analysis recommendation")
			}

			if !strings.Contains(message, test.want) ||
				!strings.Contains(message, "selected Volcano source and scheduler logs manually") {
				t.Fatalf("message=%q", message)
			}
		})
	}

	if message, ok := llmFallbackRecommendation(errors.New("provider failed"), config); ok || message != "" {
		t.Fatalf("unexpected recommendation ok=%t message=%q", ok, message)
	}
}

func TestToolCallBudgetScalesWithConfiguredRounds(t *testing.T) {
	var requests atomic.Int32

	config := llmTestConfig(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		round := requests.Add(1)
		if round <= 3 {
			writeLLMResponse(writer, map[string]any{
				"role":    "assistant",
				"content": nil,
				"tool_calls": []map[string]any{
					{
						"id":   fmt.Sprintf("call-%d-a", round),
						"type": "function",
						"function": map[string]any{
							"name":      getTargetPodToolName,
							"arguments": fmt.Sprintf(`{"round":%d,"call":"a"}`, round),
						},
					},
					{
						"id":   fmt.Sprintf("call-%d-b", round),
						"type": "function",
						"function": map[string]any{
							"name":      getTargetPodToolName,
							"arguments": fmt.Sprintf(`{"round":%d,"call":"b"}`, round),
						},
					},
				},
			})

			return
		}

		writeLLMResponse(writer, map[string]any{
			"role":    "assistant",
			"content": "answer",
		})
	}))
	config.MaxToolRounds = 3

	answer, err := config.complete(
		context.Background(),
		"diagnose",
		readOnlyKubernetesTools,
		func(context.Context, chatToolCall) string {
			return encodeToolSuccess(map[string]bool{"ok": true})
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if answer != "answer" || requests.Load() != 4 {
		t.Fatalf("answer=%q requests=%d", answer, requests.Load())
	}
}

func TestReadOnlyKubernetesToolBoundaries(t *testing.T) {
	want := map[string]bool{
		getTargetPodToolName:       true,
		listTargetEventsToolName:   true,
		listPodGroupEventsToolName: true,
		getNodeToolName:            true,
		getSchedulerLogsToolName:   true,
		readVolcanoSourceToolName:  true,
	}

	if len(readOnlyKubernetesTools) != len(want) {
		t.Fatalf("tools=%d", len(readOnlyKubernetesTools))
	}

	for _, tool := range readOnlyKubernetesTools {
		if tool.Type != "function" || !want[tool.Function.Name] {
			t.Fatalf("unexpected tool=%+v", tool)
		}

		lower := strings.ToLower(tool.Function.Name)
		for _, forbidden := range []string{"exec", "signal", "dump", "secret", "create", "update", "delete", "patch"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("tool %q contains forbidden operation %q", lower, forbidden)
			}
		}
	}

	if _, err := newKubernetesToolSession(nil, KubernetesToolScope{
		Namespace: "workload",
		Pod:       "pending-pod",
	}); err == nil || !strings.Contains(err.Error(), "cluster manager is nil") {
		t.Fatalf("nil manager err=%v", err)
	}

	if _, err := newKubernetesToolSession(&cluster.Client{}, KubernetesToolScope{}); err == nil ||
		!strings.Contains(err.Error(), "requires namespace and pod") {
		t.Fatalf("empty scope err=%v", err)
	}

	session := &kubernetesToolSession{
		allowedNodes: map[string]struct{}{"node-a": {}},
	}

	tests := []struct {
		name string
		call chatToolCall
		want string
	}{
		{
			name: "unknown argument",
			call: toolCall(getNodeToolName, `{"name":"node-a","namespace":"other"}`),
			want: "unknown field",
		},
		{
			name: "node outside analysis",
			call: toolCall(getNodeToolName, `{"name":"node-b"}`),
			want: "outside this analysis",
		},
		{
			name: "event limit",
			call: toolCall(listTargetEventsToolName, `{"limit":51}`),
			want: "between 1 and 50",
		},
		{
			name: "PodGroup event limit",
			call: toolCall(listPodGroupEventsToolName, `{"limit":51}`),
			want: "between 1 and 50",
		},
		{
			name: "scheduler log limit",
			call: toolCall(getSchedulerLogsToolName, `{"tailLines":1001}`),
			want: "between 1 and 1000",
		},
		{
			name: "source outside index",
			call: toolCall(readVolcanoSourceToolName, `{"path":"../../secret"}`),
			want: "outside the selected worktree",
		},
		{
			name: "unknown tool",
			call: toolCall("k8s_get_secret", `{}`),
			want: "is not allowed",
		},
		{
			name: "argument size",
			call: toolCall(getTargetPodToolName, strings.Repeat("x", maxToolArguments+1)),
			want: "size limit",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := session.execute(context.Background(), test.call)
			if !strings.Contains(result, `"ok":false`) || !strings.Contains(result, test.want) {
				t.Fatalf("result=%s", result)
			}
		})
	}
}

func TestReadScopedSourceFile(t *testing.T) {
	root := t.TempDir()
	path := "pkg/scheduler/plugins/gang/gang.go"
	absolute := filepath.Join(root, filepath.FromSlash(path))

	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(absolute, []byte("package gang\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	content, err := readScopedSourceFile(root, path, map[string]struct{}{path: {}})
	if err != nil {
		t.Fatal(err)
	}

	if content != "package gang\n" {
		t.Fatalf("content=%q", content)
	}

	if _, err := readScopedSourceFile(root, "pkg/scheduler/plugins/other.go", map[string]struct{}{path: {}}); err == nil ||
		!strings.Contains(err.Error(), "not in the selected hook index") {
		t.Fatalf("unindexed source err=%v", err)
	}
}

func TestReadScopedSourceFileRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	external := filepath.Join(t.TempDir(), "external.go")

	if err := os.WriteFile(external, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}

	path := "pkg/scheduler/plugins/external.go"
	absolute := filepath.Join(root, filepath.FromSlash(path))

	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(external, absolute); err != nil {
		t.Fatal(err)
	}

	_, err := readScopedSourceFile(root, path, map[string]struct{}{path: {}})
	if err == nil || !strings.Contains(err.Error(), "escapes the selected worktree") {
		t.Fatalf("err=%v", err)
	}
}

func TestSchedulingViewsRedactUnrelatedPodAndNodeData(t *testing.T) {
	pod := schedulingViewTestPod()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-a",
			Labels: map[string]string{
				"gpu-type":             "A",
				"kubernetes.io/arch":   "amd64",
				"private.example/rack": "must-not-leak",
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("8"),
			},
		},
	}

	podJSON, err := json.Marshal(summarizePod(pod))
	if err != nil {
		t.Fatal(err)
	}

	nodeJSON, err := json.Marshal(summarizeNode(node, pod))
	if err != nil {
		t.Fatal(err)
	}

	for _, forbidden := range []string{"TOP_SECRET", "private.example/token", "ordinary-label"} {
		if strings.Contains(string(podJSON), forbidden) {
			t.Errorf("pod view leaked %q: %s", forbidden, podJSON)
		}
	}

	if !strings.Contains(string(podJSON), "volcano.sh/job-name") ||
		!strings.Contains(string(podJSON), "scheduling.volcano.sh/queue-name") {
		t.Errorf("pod scheduling metadata missing: %s", podJSON)
	}

	if strings.Contains(string(nodeJSON), "private.example/rack") ||
		!strings.Contains(string(nodeJSON), `"gpu-type":"A"`) {
		t.Errorf("node view=%s", nodeJSON)
	}
}

func TestSummarizeEventsKeepsNewestAndBoundsMessage(t *testing.T) {
	oldTime := metav1.NewTime(time.Unix(100, 0))
	newTime := metav1.NewTime(time.Unix(200, 0))
	events := []corev1.Event{
		{
			Reason:        "Old",
			Message:       "old",
			LastTimestamp: oldTime,
		},
		{
			Reason:        "New",
			Message:       strings.Repeat("x", 5000),
			LastTimestamp: newTime,
		},
	}

	result := summarizeEvents(events, 1)
	if len(result) != 1 || result[0].Reason != "New" || len(result[0].Message) > 4100 {
		t.Fatalf("result=%+v", result)
	}
}

func TestChatCompletionURL(t *testing.T) {
	tests := map[string]string{
		"http://llm.example":                     "http://llm.example/v1/chat/completions",
		"http://llm.example/":                    "http://llm.example/v1/chat/completions",
		"http://llm.example/v1":                  "http://llm.example/v1/chat/completions",
		"http://llm.example/v1/":                 "http://llm.example/v1/chat/completions",
		"http://llm.example/v1/chat/completions": "http://llm.example/v1/chat/completions",
	}

	for input, want := range tests {
		if got := chatCompletionURL(input); got != want {
			t.Errorf("chatCompletionURL(%q)=%q, want %q", input, got, want)
		}
	}
}

func schedulingViewTestPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "workload",
			Name:      "pending-pod",
			UID:       "pod-uid",
			Labels: map[string]string{
				"volcano.sh/job-name": "job-a",
				"ordinary-label":      "must-not-leak",
			},
			Annotations: map[string]string{
				"scheduling.volcano.sh/queue-name": "queue-a",
				"private.example/token":            "must-not-leak",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: "volcano",
			NodeSelector: map[string]string{
				"gpu-type": "A",
			},
			Containers: []corev1.Container{
				{
					Name: "worker",
					Env: []corev1.EnvVar{
						{Name: "TOKEN", Value: "TOP_SECRET"},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}
}

func toolCall(name, arguments string) chatToolCall {
	return chatToolCall{
		ID:   "call-test",
		Type: "function",
		Function: chatToolCallFunction{
			Name:      name,
			Arguments: arguments,
		},
	}
}

func writeLLMResponse(writer http.ResponseWriter, message any) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"choices": []map[string]any{
			{"message": message},
		},
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func llmTestConfig(handler http.Handler) LLMConfig {
	return LLMConfig{
		URL:   "http://llm.example/v1",
		Model: "test-model",
		httpClient: &http.Client{
			Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)

				return recorder.Result(), nil
			}),
		},
	}
}

func dereference(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}
