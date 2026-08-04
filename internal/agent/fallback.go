package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/volcano-sh/volens/internal/cluster"
	"github.com/volcano-sh/volens/internal/source"
)

const (
	promptSourcePerFileLimit = 16 << 10
	promptSourceTotalLimit   = 192 << 10
)

func (a *Agent) applySourceFallback(
	ctx context.Context,
	report *Report,
	scheduler cluster.Scheduler,
) {
	report.SourceFallback = true

	if a.sourceManager == nil {
		report.LLM = "source preparation failed: source manager is not configured"

		return
	}

	worktree, err := a.prepareSource(ctx, report.Request, scheduler.Tag)
	if err != nil {
		report.LLM = "source preparation failed: " + err.Error()

		return
	}

	prompt, sourceFiles, err := buildPrompt(*report, worktree)
	if err != nil {
		report.LLM = "build LLM prompt failed: " + err.Error()

		return
	}

	nodeNames := make([]string, 0, len(report.Nodes))

	for _, node := range report.Nodes {
		nodeNames = append(nodeNames, node.Name)
	}

	answer, err := a.llm.CompleteWithTools(
		ctx,
		prompt,
		a.clusterManager,
		KubernetesToolScope{
			Namespace:   report.Request.Namespace,
			Pod:         report.Request.Pod,
			NodeNames:   nodeNames,
			SourceRoot:  worktree,
			SourceFiles: sourceFiles,
		},
	)
	if err == nil {
		report.LLM = answer
		report.Conclusion = answer

		return
	}

	if message, recommendManualAnalysis := llmFallbackRecommendation(err, a.llm); recommendManualAnalysis {
		report.LLM = message
		report.Conclusion = message

		return
	}

	report.LLM = "LLM fallback failed: " + err.Error()
}

func llmFallbackRecommendation(err error, config LLMConfig) (string, bool) {
	if errors.Is(err, ErrLLMToolRoundLimit) {
		return fmt.Sprintf(
			"LLM reached the configured maximum of %d tool rounds without a conclusion; review the selected Volcano source and scheduler logs manually.",
			config.maxToolRounds(),
		), true
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Sprintf(
			"LLM fallback reached a deadline before producing a conclusion (configured total timeout %s); review the selected Volcano source and scheduler logs manually.",
			config.completionTimeout(),
		), true
	}

	if errors.Is(err, context.Canceled) {
		return "LLM fallback was canceled before producing a conclusion; review the selected Volcano source and scheduler logs manually.", true
	}

	return "", false
}

func (a *Agent) prepareSource(
	ctx context.Context,
	request Request,
	schedulerTag string,
) (string, error) {
	if request.Branch != "" {
		return a.sourceManager.PrepareBranch(ctx, request.Branch)
	}

	return a.sourceManager.Prepare(ctx, schedulerTag)
}

func buildPrompt(report Report, worktree string) (string, []string, error) {
	reportJSON, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", nil, fmt.Errorf("encode analysis report: %w", err)
	}

	sources, err := source.LoadSchedulerSources(
		worktree,
		promptSourcePerFileLimit,
		promptSourceTotalLimit,
	)
	if err != nil {
		return "", nil, fmt.Errorf("index scheduler hook source: %w", err)
	}

	paths := make([]string, 0, len(sources))
	var sourceIndex strings.Builder
	var sourceText strings.Builder

	for _, file := range sources {
		paths = append(paths, file.Path)

		hooks := "none"
		if len(file.Hooks) > 0 {
			hooks = strings.Join(file.Hooks, ",")
		}

		_, _ = fmt.Fprintf(
			&sourceIndex,
			"- %s hooks=%s content_in_prompt=%t\n",
			file.Path,
			hooks,
			file.Content != "",
		)

		if file.Content != "" {
			sourceText.WriteString("\nFILE " + file.Path + "\n" + file.Content)
		}
	}

	prompt := "You diagnose Volcano Pod Pending. Use only supplied evidence and source. " +
		"State root cause, exact function path, and next verification. " +
		"Do not invent runtime facts. A passed common rule does not prove a branch-specific plugin passed. " +
		"An indexed plugin is only a source candidate; source presence does not prove it is enabled in the runtime scheduler tiers or actions. " +
		"JobEnqueueable uses configured tier voting and short-circuiting, not a flat AND. " +
		"The source index was discovered from the exact selected worktree; use the source read tool for indexed files whose content is absent or truncated.\nREPORT\n" +
		string(reportJSON) + "\nSOURCE INDEX\n" + sourceIndex.String() +
		"\nBOUNDED SOURCE CONTENT\n" + sourceText.String()

	return prompt, paths, nil
}
