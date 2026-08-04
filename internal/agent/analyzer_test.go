package agent

import (
	"context"
	"strings"
	"testing"
)

func TestAnalyzeRequiresConcreteClusterManager(t *testing.T) {
	agent := New(nil, nil, LLMConfig{})

	_, err := agent.Analyze(context.Background(), Request{
		Namespace: "default",
		Pod:       "pod-a",
	})
	if err == nil || !strings.Contains(err.Error(), "cluster manager") {
		t.Fatalf("err=%v", err)
	}
}
