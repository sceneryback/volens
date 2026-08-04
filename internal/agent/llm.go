package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	llmRoundTimeoutBudget   = 2 * time.Minute
	maxLLMResponseBytes     = 4 << 20
	defaultMaxToolRounds    = 4
	maximumMaxToolRounds    = 16
	maxToolResultsTotalSize = 256 << 10
)

const llmSystemPrompt = "You are a precise Volcano scheduler debugging agent."

const llmToolSystemPrompt = ` Read-only, analysis-scoped tools are available for facts that are not already established by the report. They can inspect the selected Pod and PodGroup, report nodes, the current Volcano scheduler log tail, and dynamically indexed source files from the exact selected branch or tag. Tool results are additional supplied evidence. A failed tool call means that fact is unknown; it is not proof of a scheduling failure. Never request mutation, exec, signals, secrets, or data outside the selected analysis.`

var ErrLLMToolRoundLimit = errors.New("LLM tool-call round limit exceeded")

type LLMToolRoundLimitError struct {
	CompletedRounds int
	MaxRounds       int
}

func (e *LLMToolRoundLimitError) Error() string {
	return fmt.Sprintf(
		"%s after %d completed rounds (configured maximum %d); inspect the scheduler logs and source for further analysis",
		ErrLLMToolRoundLimit,
		e.CompletedRounds,
		e.MaxRounds,
	)
}

func (e *LLMToolRoundLimitError) Unwrap() error {
	return ErrLLMToolRoundLimit
}

type LLMConfig struct {
	URL           string
	Key           string
	Model         string
	MaxToolRounds int
	Timeout       time.Duration
	httpClient    *http.Client
}

type chatCompletionRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature int           `json:"temperature"`
	Tools       []chatTool    `json:"tools,omitempty"`
	ToolChoice  string        `json:"tool_choice,omitempty"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

type chatMessage struct {
	Role       string         `json:"role"`
	Content    *string        `json:"content"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type chatTool struct {
	Type     string           `json:"type"`
	Function chatToolFunction `json:"function"`
}

type chatToolFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

type chatToolCall struct {
	ID       string               `json:"id"`
	Type     string               `json:"type"`
	Function chatToolCallFunction `json:"function"`
}

type chatToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type chatToolExecutor func(context.Context, chatToolCall) string

func LLMConfigFromEnv() LLMConfig {
	maxToolRounds := boundedPositiveIntEnv(
		"LLM_MAX_TOOL_ROUNDS",
		defaultMaxToolRounds,
		maximumMaxToolRounds,
	)

	return LLMConfig{
		URL:           env("LLM_BASE_URL", ""),
		Key:           os.Getenv("LLM_API_KEY"),
		Model:         env("LLM_MODEL", "gpt-4.1-mini"),
		MaxToolRounds: maxToolRounds,
		Timeout: positiveDurationEnv(
			"LLM_TIMEOUT",
			defaultLLMCompletionTimeout(maxToolRounds),
		),
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func boundedPositiveIntEnv(key string, fallback, maximum int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}

	if parsed > maximum {
		return maximum
	}

	return parsed
}

func positiveDurationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}

	return parsed
}

func defaultLLMCompletionTimeout(maxToolRounds int) time.Duration {
	if maxToolRounds <= 0 {
		maxToolRounds = defaultMaxToolRounds
	}

	if maxToolRounds > maximumMaxToolRounds {
		maxToolRounds = maximumMaxToolRounds
	}

	return time.Duration(maxToolRounds+1) * llmRoundTimeoutBudget
}

func (c LLMConfig) maxToolRounds() int {
	if c.MaxToolRounds <= 0 {
		return defaultMaxToolRounds
	}

	if c.MaxToolRounds > maximumMaxToolRounds {
		return maximumMaxToolRounds
	}

	return c.MaxToolRounds
}

func (c LLMConfig) completionTimeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}

	return defaultLLMCompletionTimeout(c.maxToolRounds())
}

func (c LLMConfig) Complete(ctx context.Context, prompt string) (string, error) {
	return c.complete(ctx, prompt, nil, nil)
}

func (c LLMConfig) complete(
	ctx context.Context,
	prompt string,
	tools []chatTool,
	execute chatToolExecutor,
) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("LLM context is nil")
	}

	if c.URL == "" {
		return "", fmt.Errorf("LLM_BASE_URL is not configured")
	}

	systemPrompt := llmSystemPrompt
	if len(tools) > 0 {
		systemPrompt += llmToolSystemPrompt
	}

	messages := []chatMessage{
		{
			Role:    "system",
			Content: stringPointer(systemPrompt),
		},
		{
			Role:    "user",
			Content: stringPointer(prompt),
		},
	}

	completionCtx, cancel := context.WithTimeout(ctx, c.completionTimeout())
	defer cancel()

	client := c.httpClient
	if client == nil {
		client = &http.Client{}
	}

	toolResults := map[string]string{}
	totalToolCalls := 0
	totalToolResultSize := 0
	toolRounds := 0
	maxToolRounds := c.maxToolRounds()
	maxToolCalls := maxToolRounds * len(tools)

	for {
		message, err := c.chat(completionCtx, client, messages, tools)
		if err != nil {
			return "", err
		}

		if len(message.ToolCalls) == 0 {
			if message.Content == nil || strings.TrimSpace(*message.Content) == "" {
				return "", fmt.Errorf("LLM returned neither content nor tool calls")
			}

			return *message.Content, nil
		}

		if execute == nil || len(tools) == 0 {
			return "", fmt.Errorf("LLM returned tool calls when no tools were enabled")
		}

		if toolRounds >= maxToolRounds {
			return "", &LLMToolRoundLimitError{
				CompletedRounds: toolRounds,
				MaxRounds:       maxToolRounds,
			}
		}

		if totalToolCalls+len(message.ToolCalls) > maxToolCalls {
			return "", fmt.Errorf("LLM tool-call count limit exceeded")
		}

		if message.Role == "" {
			message.Role = "assistant"
		}

		if message.Role != "assistant" {
			return "", fmt.Errorf("LLM tool-call message has role %q", message.Role)
		}

		for _, call := range message.ToolCalls {
			if call.ID == "" || call.Type != "function" || call.Function.Name == "" {
				return "", fmt.Errorf("LLM returned an invalid tool call")
			}
		}

		messages = append(messages, message)
		toolRounds++
		totalToolCalls += len(message.ToolCalls)

		for _, call := range message.ToolCalls {
			cacheKey := call.Function.Name + "\x00" + call.Function.Arguments
			content, found := toolResults[cacheKey]

			if !found {
				content = execute(completionCtx, call)
				toolResults[cacheKey] = content
			}

			if totalToolResultSize+len(content) > maxToolResultsTotalSize {
				content = encodeToolError("tool result budget exceeded")
			}

			totalToolResultSize += len(content)

			messages = append(messages, chatMessage{
				Role:       "tool",
				Content:    stringPointer(content),
				ToolCallID: call.ID,
			})
		}
	}
}

func (c LLMConfig) chat(
	ctx context.Context,
	client *http.Client,
	messages []chatMessage,
	tools []chatTool,
) (chatMessage, error) {
	body := chatCompletionRequest{
		Model:       c.Model,
		Messages:    messages,
		Temperature: 0,
		Tools:       tools,
	}

	if len(tools) > 0 {
		body.ToolChoice = "auto"
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return chatMessage{}, fmt.Errorf("encode LLM request: %w", err)
	}

	url := chatCompletionURL(c.URL)

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return chatMessage{}, fmt.Errorf("create LLM request: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")
	if c.Key != "" {
		request.Header.Set("Authorization", "Bearer "+c.Key)
	}

	response, err := client.Do(request)
	if err != nil {
		return chatMessage{}, fmt.Errorf("send LLM request: %w", err)
	}

	defer response.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(response.Body, maxLLMResponseBytes+1))
	if err != nil {
		return chatMessage{}, fmt.Errorf("read LLM response: %w", err)
	}

	if len(raw) > maxLLMResponseBytes {
		return chatMessage{}, fmt.Errorf("LLM response exceeds %d bytes", maxLLMResponseBytes)
	}

	if response.StatusCode/100 != 2 {
		return chatMessage{}, fmt.Errorf("LLM HTTP %s: %s", response.Status, string(raw))
	}

	var result chatCompletionResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return chatMessage{}, fmt.Errorf("decode LLM response: %w", err)
	}

	if len(result.Choices) == 0 {
		return chatMessage{}, fmt.Errorf("empty LLM response")
	}

	return result.Choices[0].Message, nil
}

func stringPointer(value string) *string {
	return &value
}

func chatCompletionURL(baseURL string) string {
	url := strings.TrimRight(baseURL, "/")

	if strings.HasSuffix(url, "/chat/completions") {
		return url
	}

	if strings.HasSuffix(url, "/v1") {
		return url + "/chat/completions"
	}

	return url + "/v1/chat/completions"
}
