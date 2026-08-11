package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	runtimeanalysis "github.com/volcano-sh/volens/internal/agent/runtime"
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
	evidence presentationEvidence,
) {
	report.SourceFallback = true

	if a.sourceManager == nil {
		report.LLM = "source preparation failed: source manager is not configured"
		finishReport(report, evidence)

		return
	}

	worktree, err := a.prepareSource(ctx, report.Request, scheduler)
	if err != nil {
		report.LLM = "source preparation failed: " + err.Error()
		finishReport(report, evidence)

		return
	}

	sources, err := source.LoadSchedulerSources(
		worktree,
		promptSourcePerFileLimit,
		promptSourceTotalLimit,
	)
	if err != nil {
		report.LLM = "index selected Volcano source failed: " + err.Error()
		finishReport(report, evidence)

		return
	}

	defaults, defaultsErr := source.LoadSchedulerPluginDefaults(worktree)
	predicateDefaults, predicateDefaultsErr := source.LoadPredicatePluginDefaults(worktree)
	report.PredicateDefaults = predicateDefaults
	if predicateDefaultsErr != nil {
		report.PredicateDefaultsErr = predicateDefaultsErr.Error()
	}

	report.PluginHooks = runtimeanalysis.InspectPluginHooks(
		report.Policy,
		sources,
		defaults,
		defaultsErr,
	)
	report.HooksInspected = true
	finishReport(report, evidence)

	if len(knownFailures(*report)) > 0 {
		return
	}

	if !hasUnknown(*report) {
		return
	}

	prompt, sourceFiles, err := buildPromptFromSources(*report, sources)
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
		report.Diagnosis = Diagnosis{
			RootCause: "LLM 基于所选分支源码和运行时证据给出的分析：" + answer,
			Suggestions: []string{
				"按上述函数路径和 scheduler 日志证据复核后再修改集群配置",
			},
		}

		return
	}

	if message, recommendManualAnalysis := llmFallbackRecommendation(err, a.llm); recommendManualAnalysis {
		report.LLM = message
		report.Conclusion = message
		report.Diagnosis = Diagnosis{
			RootCause: "自动分析未能在配置的 LLM 循环或时间限制内确定根因",
			Suggestions: []string{
				message,
				"根据页面中的 scheduler 日志和所选分支源码继续人工分析",
			},
		}

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
	scheduler cluster.Scheduler,
) (string, error) {
	if request.Branch != "" {
		log.Printf(
			"preparing Volcano source from user-selected branch namespace=%s pod=%s branch=%s",
			request.Namespace,
			request.Pod,
			request.Branch,
		)

		return a.sourceManager.PrepareBranch(ctx, request.Branch)
	}

	versioned, versionErr := a.clusterManager.GetVolcanoSchedulerVersionFor(ctx, scheduler)
	if versionErr == nil && versioned.Version != "" {
		log.Printf(
			"preparing Volcano source from detected scheduler version scheduler=%s/%s container=%s version=%s gitSHA=%s imageTag=%s",
			versioned.Namespace,
			versioned.Name,
			versioned.Container,
			versioned.Version,
			versioned.GitSHA,
			versioned.Tag,
		)

		return a.sourceManager.Prepare(ctx, versioned.Version)
	}

	log.Printf(
		"preparing Volcano source from scheduler image tag scheduler=%s/%s tag=%s versionErr=%v",
		scheduler.Namespace,
		scheduler.Name,
		scheduler.Tag,
		versionErr,
	)

	return a.sourceManager.Prepare(ctx, scheduler.Tag)
}

func buildPrompt(report Report, worktree string) (string, []string, error) {
	sources, err := source.LoadSchedulerSources(
		worktree,
		promptSourcePerFileLimit,
		promptSourceTotalLimit,
	)
	if err != nil {
		return "", nil, fmt.Errorf("index scheduler hook source: %w", err)
	}

	return buildPromptFromSources(report, sources)
}

func buildPromptFromSources(
	report Report,
	sources []source.SourceFile,
) (string, []string, error) {
	reportJSON, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", nil, fmt.Errorf("encode analysis report: %w", err)
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

		plugins := "none"
		if len(file.PluginNames) > 0 {
			plugins = strings.Join(file.PluginNames, ",")
		}

		_, _ = fmt.Fprintf(
			&sourceIndex,
			"- %s plugins=%s hooks=%s content_in_prompt=%t\n",
			file.Path,
			plugins,
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
