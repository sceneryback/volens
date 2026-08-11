package source

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	schedulerActionsDirectory   = "pkg/scheduler/actions"
	schedulerPluginsDirectory   = "pkg/scheduler/plugins"
	maximumSchedulerSourceFiles = 4096
	maximumSchedulerSourceFile  = 2 << 20
	maximumSchedulerSourceScan  = 64 << 20
)

var schedulerFrameworkFiles = []string{
	"cmd/scheduler/app/options/options.go",
	"pkg/scheduler/actions/factory.go",
	"pkg/scheduler/actions/allocate/allocate.go",
	"pkg/scheduler/actions/enqueue/enqueue.go",
	"pkg/scheduler/conf/scheduler_conf.go",
	"pkg/scheduler/api/job_info.go",
	"pkg/scheduler/cache/event_handlers.go",
	"pkg/scheduler/framework/session.go",
	"pkg/scheduler/framework/session_plugins.go",
	"pkg/scheduler/plugins/defaults.go",
	"pkg/scheduler/plugins/factory.go",
	"pkg/scheduler/plugins/util/util.go",
	"pkg/scheduler/util/priority_queue.go",
}

var schedulerHookNames = []string{
	"AddJobOrderFn",
	"AddQueueOrderFn",
	"AddTaskOrderFn",
	"AddPreemptableFn",
	"AddReclaimableFn",
	"AddJobReadyFn",
	"AddJobPipelinedFn",
	"AddJobValidFn",
	"AddJobEnqueueableFn",
	"AddJobEnqueuedFn",
	"AddPrePredicateFn",
	"AddPredicateFn",
	"AddBestNodeFn",
	"AddNodeOrderFn",
	"AddBatchNodeOrderFn",
	"AddNodeMapFn",
	"AddNodeReduceFn",
	"AddOverusedFn",
	"AddPreemptiveFn",
	"AddAllocatableFn",
	"AddTargetJobFn",
	"AddReservedNodesFn",
	"AddVictimTasksFns",
	"AddJobStarvingFns",
}

// SourceFile describes a scheduler source file from the selected Volcano
// worktree. Hooks is empty for the fixed action and framework files.
type SourceFile struct {
	Path        string
	Hooks       []string
	PluginNames []string
	Content     string
}

type schedulerSourceCandidate struct {
	path        string
	hooks       []string
	pluginNames []string
	content     []byte
}

// LoadSchedulerSources discovers the scheduler actions, framework dispatch,
// and plugin hook registrations in an already prepared Volcano worktree.
// Returned paths are repository-relative and sorted. Source content is bounded
// independently per file and across the full result; paths and hooks are still
// returned after the content budget is exhausted.
func LoadSchedulerSources(
	worktree string,
	perFileLimit int,
	totalLimit int,
) ([]SourceFile, error) {
	if perFileLimit <= 0 {
		return nil, fmt.Errorf("scheduler source per-file limit must be positive")
	}

	if totalLimit <= 0 {
		return nil, fmt.Errorf("scheduler source total limit must be positive")
	}

	root, err := schedulerSourceRoot(worktree)
	if err != nil {
		return nil, err
	}

	candidates := make(map[string]schedulerSourceCandidate)

	for _, path := range schedulerFrameworkFiles {
		candidate, found, err := loadSchedulerSource(root, path)
		if err != nil {
			return nil, err
		}

		if found {
			candidates[path] = candidate
		}
	}

	actions, err := discoverSchedulerActionSources(root)
	if err != nil {
		return nil, err
	}

	for _, candidate := range actions {
		candidates[candidate.path] = candidate
	}

	plugins, err := discoverPluginSources(root)
	if err != nil {
		return nil, err
	}

	for _, candidate := range plugins {
		candidates[candidate.path] = candidate
	}

	paths := make([]string, 0, len(candidates))

	for path := range candidates {
		paths = append(paths, path)
	}

	sort.Strings(paths)

	result := make([]SourceFile, 0, len(paths))
	remaining := totalLimit

	for _, path := range paths {
		candidate := candidates[path]
		contentLimit := min(perFileLimit, remaining, len(candidate.content))

		result = append(result, SourceFile{
			Path:        candidate.path,
			Hooks:       append([]string(nil), candidate.hooks...),
			PluginNames: append([]string(nil), candidate.pluginNames...),
			Content:     string(candidate.content[:contentLimit]),
		})

		remaining -= contentLimit
	}

	return result, nil
}

func discoverSchedulerActionSources(root string) ([]schedulerSourceCandidate, error) {
	actionsRoot := filepath.Join(root, filepath.FromSlash(schedulerActionsDirectory))
	info, err := os.Lstat(actionsRoot)

	if os.IsNotExist(err) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("inspect Volcano scheduler actions: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("Volcano scheduler actions directory must not be a symbolic link")
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("Volcano scheduler actions path is not a directory")
	}

	resolvedActionsRoot, err := filepath.EvalSymlinks(actionsRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve Volcano scheduler actions path: %w", err)
	}

	if filepath.Clean(resolvedActionsRoot) != filepath.Clean(actionsRoot) {
		return nil, fmt.Errorf("Volcano scheduler actions path contains a symbolic link")
	}

	candidates := make([]schedulerSourceCandidate, 0)
	totalBytes := 0

	err = filepath.WalkDir(actionsRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("scheduler action source path %s is a symbolic link", path)
		}

		if entry.IsDir() {
			return nil
		}

		if filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}

		if len(candidates) >= maximumSchedulerSourceFiles {
			return fmt.Errorf("scheduler action source count exceeds %d", maximumSchedulerSourceFiles)
		}

		relative, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("resolve scheduler action source path: %w", err)
		}

		repositoryPath := filepath.ToSlash(relative)
		candidate, found, err := loadSchedulerSource(root, repositoryPath)
		if err != nil {
			return err
		}

		if !found {
			return nil
		}

		totalBytes += len(candidate.content)
		if totalBytes > maximumSchedulerSourceScan {
			return fmt.Errorf("scheduler action source exceeds %d total bytes", maximumSchedulerSourceScan)
		}

		candidates = append(candidates, candidate)

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan Volcano scheduler actions: %w", err)
	}

	return candidates, nil
}

func schedulerSourceRoot(worktree string) (string, error) {
	if strings.TrimSpace(worktree) == "" {
		return "", fmt.Errorf("Volcano worktree is empty")
	}

	info, err := os.Lstat(worktree)
	if err != nil {
		return "", fmt.Errorf("inspect Volcano worktree: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("Volcano worktree must not be a symbolic link")
	}

	if !info.IsDir() {
		return "", fmt.Errorf("Volcano worktree is not a directory")
	}

	absolute, err := filepath.Abs(worktree)
	if err != nil {
		return "", fmt.Errorf("resolve Volcano worktree path: %w", err)
	}

	root, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve Volcano worktree links: %w", err)
	}

	return filepath.Clean(root), nil
}

func discoverPluginSources(root string) ([]schedulerSourceCandidate, error) {
	pluginsRoot := filepath.Join(root, filepath.FromSlash(schedulerPluginsDirectory))
	info, err := os.Lstat(pluginsRoot)

	if os.IsNotExist(err) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("inspect Volcano scheduler plugins: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("Volcano scheduler plugins directory must not be a symbolic link")
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("Volcano scheduler plugins path is not a directory")
	}

	resolvedPluginsRoot, err := filepath.EvalSymlinks(pluginsRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve Volcano scheduler plugins path: %w", err)
	}

	if filepath.Clean(resolvedPluginsRoot) != filepath.Clean(pluginsRoot) {
		return nil, fmt.Errorf("Volcano scheduler plugins path contains a symbolic link")
	}

	allCandidates := make(map[string]schedulerSourceCandidate)
	registeredUnits := make(map[string]struct{})
	packagePluginNames := make(map[string]map[string]struct{})
	totalBytes := 0

	err = filepath.WalkDir(pluginsRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("scheduler plugin source path %s is a symbolic link", path)
		}

		if entry.IsDir() {
			return nil
		}

		if filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}

		if len(allCandidates) >= maximumSchedulerSourceFiles {
			return fmt.Errorf("scheduler plugin source count exceeds %d", maximumSchedulerSourceFiles)
		}

		relative, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("resolve scheduler plugin source path: %w", err)
		}

		repositoryPath := filepath.ToSlash(relative)
		candidate, found, err := loadSchedulerSource(root, repositoryPath)
		if err != nil {
			return err
		}

		if !found {
			return nil
		}

		totalBytes += len(candidate.content)
		if totalBytes > maximumSchedulerSourceScan {
			return fmt.Errorf("scheduler plugin source exceeds %d total bytes", maximumSchedulerSourceScan)
		}

		candidate.hooks, candidate.pluginNames, err = schedulerPluginMetadata(
			candidate.content,
			repositoryPath,
		)
		if err != nil {
			return err
		}

		allCandidates[candidate.path] = candidate
		unit := pluginPackage(candidate.path)
		packagePath := pluginPackage(candidate.path)

		if packagePluginNames[packagePath] == nil {
			packagePluginNames[packagePath] = map[string]struct{}{}
		}

		for _, name := range candidate.pluginNames {
			packagePluginNames[packagePath][name] = struct{}{}
		}

		if len(candidate.hooks) > 0 {
			registeredUnits[unit] = struct{}{}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan Volcano scheduler plugins: %w", err)
	}

	candidates := make([]schedulerSourceCandidate, 0, len(allCandidates))

	for _, candidate := range allCandidates {
		owner, found := nearestRegisteredPluginUnit(candidate.path, registeredUnits)
		if !found {
			continue
		}

		candidate.pluginNames = sortedStringSet(packagePluginNames[owner])
		candidates = append(candidates, candidate)

	}

	return candidates, nil
}

func pluginPackage(repositoryPath string) string {
	return filepath.ToSlash(filepath.Dir(filepath.FromSlash(repositoryPath)))
}

func nearestRegisteredPluginUnit(
	repositoryPath string,
	registeredUnits map[string]struct{},
) (string, bool) {
	unit := pluginPackage(repositoryPath)

	for unit == schedulerPluginsDirectory || strings.HasPrefix(unit, schedulerPluginsDirectory+"/") {
		if _, found := registeredUnits[unit]; found {
			return unit, true
		}

		if unit == schedulerPluginsDirectory {
			break
		}

		parent := filepath.ToSlash(filepath.Dir(filepath.FromSlash(unit)))
		if parent == unit {
			break
		}

		unit = parent
	}

	return "", false
}

func loadSchedulerSource(
	root string,
	repositoryPath string,
) (schedulerSourceCandidate, bool, error) {
	cleanPath := filepath.Clean(filepath.FromSlash(repositoryPath))

	if filepath.IsAbs(cleanPath) || cleanPath == ".." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) {
		return schedulerSourceCandidate{}, false, fmt.Errorf(
			"scheduler source path %q escapes the Volcano worktree",
			repositoryPath,
		)
	}

	path := filepath.Join(root, cleanPath)
	info, err := os.Lstat(path)

	if os.IsNotExist(err) {
		return schedulerSourceCandidate{}, false, nil
	}

	if err != nil {
		return schedulerSourceCandidate{}, false, fmt.Errorf("inspect scheduler source %s: %w", repositoryPath, err)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return schedulerSourceCandidate{}, false, fmt.Errorf("scheduler source %s is a symbolic link", repositoryPath)
	}

	if !info.Mode().IsRegular() {
		return schedulerSourceCandidate{}, false, fmt.Errorf("scheduler source %s is not a regular file", repositoryPath)
	}

	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return schedulerSourceCandidate{}, false, fmt.Errorf("resolve scheduler source %s: %w", repositoryPath, err)
	}

	inside, err := pathInsideRoot(root, resolved)
	if err != nil {
		return schedulerSourceCandidate{}, false, err
	}

	if !inside || filepath.Clean(resolved) != filepath.Clean(path) {
		return schedulerSourceCandidate{}, false, fmt.Errorf(
			"scheduler source %s resolves outside its worktree or through a symbolic link",
			repositoryPath,
		)
	}

	if info.Size() > maximumSchedulerSourceFile {
		return schedulerSourceCandidate{}, false, fmt.Errorf(
			"scheduler source %s exceeds %d bytes",
			repositoryPath,
			maximumSchedulerSourceFile,
		)
	}

	file, err := os.Open(resolved)
	if err != nil {
		return schedulerSourceCandidate{}, false, fmt.Errorf("open scheduler source %s: %w", repositoryPath, err)
	}

	defer file.Close()

	content, err := io.ReadAll(io.LimitReader(file, maximumSchedulerSourceFile+1))
	if err != nil {
		return schedulerSourceCandidate{}, false, fmt.Errorf("read scheduler source %s: %w", repositoryPath, err)
	}

	if len(content) > maximumSchedulerSourceFile {
		return schedulerSourceCandidate{}, false, fmt.Errorf(
			"scheduler source %s exceeds %d bytes",
			repositoryPath,
			maximumSchedulerSourceFile,
		)
	}

	return schedulerSourceCandidate{
		path:    filepath.ToSlash(cleanPath),
		content: content,
	}, true, nil
}

func schedulerPluginMetadata(content []byte, path string) ([]string, []string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, content, parser.SkipObjectResolution)
	if err != nil {
		return nil, nil, fmt.Errorf("parse scheduler plugin source %s: %w", path, err)
	}

	registered := make(map[string]struct{})
	pluginNames := make(map[string]struct{})

	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		name := calledFunctionName(call.Fun)

		if !schedulerHookRegistration(name) {
			return true
		}

		registered[name] = struct{}{}

		if len(call.Args) > 0 {
			if literal, ok := call.Args[0].(*ast.BasicLit); ok && literal.Kind == token.STRING {
				value, err := strconv.Unquote(literal.Value)
				if err == nil && strings.TrimSpace(value) != "" {
					pluginNames[value] = struct{}{}
				}
			}
		}

		return true
	})

	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || (general.Tok != token.CONST && general.Tok != token.VAR) {
			continue
		}

		for _, specification := range general.Specs {
			valueSpec, ok := specification.(*ast.ValueSpec)
			if !ok {
				continue
			}

			for index, name := range valueSpec.Names {
				if name.Name != "PluginName" || index >= len(valueSpec.Values) {
					continue
				}

				literal, ok := valueSpec.Values[index].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}

				value, err := strconv.Unquote(literal.Value)
				if err == nil && strings.TrimSpace(value) != "" {
					pluginNames[value] = struct{}{}
				}
			}
		}
	}

	hooks := make([]string, 0, len(registered))

	for _, hook := range schedulerHookNames {
		if _, found := registered[hook]; found {
			hooks = append(hooks, hook)
			delete(registered, hook)
		}
	}

	unknownHooks := sortedStringSet(registered)
	hooks = append(hooks, unknownHooks...)

	return hooks, sortedStringSet(pluginNames), nil
}

func schedulerHookRegistration(name string) bool {
	return strings.HasPrefix(name, "Add") &&
		(strings.HasSuffix(name, "Fn") || strings.HasSuffix(name, "Fns"))
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))

	for value := range values {
		result = append(result, value)
	}

	sort.Strings(result)

	return result
}

func calledFunctionName(function ast.Expr) string {
	switch typed := function.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return typed.Sel.Name
	default:
		return ""
	}
}

func pathInsideRoot(root, path string) (bool, error) {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false, fmt.Errorf("compare scheduler source with worktree: %w", err)
	}

	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)), nil
}
