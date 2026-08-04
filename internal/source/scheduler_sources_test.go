package source

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadSchedulerSourcesDiscoversFixedFilesAndRegisteredHooks(t *testing.T) {
	worktree := t.TempDir()
	files := map[string]string{
		"pkg/scheduler/actions/allocate/allocate.go": `package allocate

func Execute() {}
`,
		"pkg/scheduler/actions/allocate/v2/allocate.go": `package allocate

func Execute() {}
`,
		"pkg/scheduler/actions/enqueue/enqueue.go": `package enqueue

func Execute() {}
`,
		"pkg/scheduler/actions/enqueue/v2/enqueue.go": `package enqueue

func Execute() {}
`,
		"pkg/scheduler/framework/session_plugins.go": `package framework

func Dispatch() {}
`,
		"pkg/scheduler/plugins/alpha/register.go": `package alpha

func register(session Session) {
	session.AddJobEnqueueableFn("alpha", enqueue)
	AddPrePredicateFn("alpha", prePredicate)
}
`,
		"pkg/scheduler/plugins/alpha/helper/helper.go": `package helper

func Evaluate() {}
`,
		"pkg/scheduler/plugins/beta/register.go": `package beta

func register(session Session) {
	session.AddJobValidFn("beta", validate)
	session.AddPredicateFn("beta", predicate)
	session.AddPredicateFn("beta-duplicate", predicate)
}
`,
		"pkg/scheduler/plugins/comment/comment.go": `package comment

// session.AddJobValidFn("comment", validate)
const example = "AddPredicateFn"
`,
		"pkg/scheduler/plugins/ignored/ignored.go": `package ignored

func register(session Session) {
	session.AddNodeOrderFn("ignored", order)
}
`,
		"pkg/scheduler/plugins/testonly/register_test.go": `package testonly

func register(session Session) {
	session.AddJobValidFn("test", validate)
}
`,
	}

	for path, content := range files {
		writeSchedulerSource(t, worktree, path, content)
	}

	sources, err := LoadSchedulerSources(worktree, 4_096, 32_768)
	if err != nil {
		t.Fatalf("LoadSchedulerSources() error = %v", err)
	}

	wantPaths := []string{
		"pkg/scheduler/actions/allocate/allocate.go",
		"pkg/scheduler/actions/allocate/v2/allocate.go",
		"pkg/scheduler/actions/enqueue/enqueue.go",
		"pkg/scheduler/actions/enqueue/v2/enqueue.go",
		"pkg/scheduler/framework/session_plugins.go",
		"pkg/scheduler/plugins/alpha/helper/helper.go",
		"pkg/scheduler/plugins/alpha/register.go",
		"pkg/scheduler/plugins/beta/register.go",
	}
	gotPaths := make([]string, 0, len(sources))

	for _, source := range sources {
		gotPaths = append(gotPaths, source.Path)
	}

	if !reflect.DeepEqual(gotPaths, wantPaths) {
		t.Fatalf("source paths = %v, want %v", gotPaths, wantPaths)
	}

	for _, source := range sources[:6] {
		if len(source.Hooks) != 0 {
			t.Fatalf("non-registration file %s hooks = %v, want empty", source.Path, source.Hooks)
		}
	}

	wantAlphaHooks := []string{"AddJobEnqueueableFn", "AddPrePredicateFn"}
	if !reflect.DeepEqual(sources[6].Hooks, wantAlphaHooks) {
		t.Fatalf("alpha hooks = %v, want %v", sources[6].Hooks, wantAlphaHooks)
	}

	wantBetaHooks := []string{"AddJobValidFn", "AddPredicateFn"}
	if !reflect.DeepEqual(sources[7].Hooks, wantBetaHooks) {
		t.Fatalf("beta hooks = %v, want %v", sources[7].Hooks, wantBetaHooks)
	}

	for _, source := range sources {
		if source.Content == "" {
			t.Errorf("source %s content is empty with an ample budget", source.Path)
		}
	}
}

func TestLoadSchedulerSourcesAppliesBudgetsWithoutDroppingIndex(t *testing.T) {
	worktree := t.TempDir()

	writeSchedulerSource(
		t,
		worktree,
		"pkg/scheduler/actions/allocate/allocate.go",
		"package allocate\n\nconst value = 123456789\n",
	)
	writeSchedulerSource(
		t,
		worktree,
		"pkg/scheduler/actions/enqueue/enqueue.go",
		"package enqueue\n\nconst value = 123456789\n",
	)
	writeSchedulerSource(
		t,
		worktree,
		"pkg/scheduler/plugins/quota/register.go",
		"package quota\n\nfunc register(s Session) { s.AddJobEnqueueableFn(\"quota\", enqueue) }\n",
	)

	sources, err := LoadSchedulerSources(worktree, 8, 13)
	if err != nil {
		t.Fatalf("LoadSchedulerSources() error = %v", err)
	}

	if len(sources) != 3 {
		t.Fatalf("source count = %d, want 3", len(sources))
	}

	if len(sources[0].Content) != 8 {
		t.Errorf("first content length = %d, want 8", len(sources[0].Content))
	}

	if len(sources[1].Content) != 5 {
		t.Errorf("second content length = %d, want 5", len(sources[1].Content))
	}

	if sources[2].Content != "" {
		t.Errorf("third content = %q, want empty after total budget exhaustion", sources[2].Content)
	}

	wantHooks := []string{"AddJobEnqueueableFn"}
	if !reflect.DeepEqual(sources[2].Hooks, wantHooks) {
		t.Fatalf("third hooks = %v, want %v", sources[2].Hooks, wantHooks)
	}
}

func TestLoadSchedulerSourcesRejectsPluginSourceSymlink(t *testing.T) {
	worktree := t.TempDir()
	external := filepath.Join(t.TempDir(), "external.go")

	if err := os.WriteFile(
		external,
		[]byte("package external\nfunc register(s Session) { s.AddPredicateFn(\"external\", predicate) }\n"),
		0o644,
	); err != nil {
		t.Fatalf("write external source: %v", err)
	}

	pluginDirectory := filepath.Join(worktree, "pkg", "scheduler", "plugins", "external")
	if err := os.MkdirAll(pluginDirectory, 0o755); err != nil {
		t.Fatalf("create plugin directory: %v", err)
	}

	if err := os.Symlink(external, filepath.Join(pluginDirectory, "external.go")); err != nil {
		t.Fatalf("create plugin source symlink: %v", err)
	}

	_, err := LoadSchedulerSources(worktree, 1_024, 4_096)
	if err == nil {
		t.Fatal("LoadSchedulerSources() error = nil, want symbolic link rejection")
	}

	if !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("LoadSchedulerSources() error = %q, want symbolic link message", err)
	}
}

func TestLoadSchedulerSourcesRejectsFixedSourceEscapingThroughSymlink(t *testing.T) {
	worktree := t.TempDir()
	externalDirectory := t.TempDir()

	if err := os.WriteFile(
		filepath.Join(externalDirectory, "allocate.go"),
		[]byte("package allocate\n"),
		0o644,
	); err != nil {
		t.Fatalf("write external action: %v", err)
	}

	actionsDirectory := filepath.Join(worktree, "pkg", "scheduler", "actions")
	if err := os.MkdirAll(actionsDirectory, 0o755); err != nil {
		t.Fatalf("create actions directory: %v", err)
	}

	if err := os.Symlink(externalDirectory, filepath.Join(actionsDirectory, "allocate")); err != nil {
		t.Fatalf("create action directory symlink: %v", err)
	}

	_, err := LoadSchedulerSources(worktree, 1_024, 4_096)
	if err == nil {
		t.Fatal("LoadSchedulerSources() error = nil, want worktree escape rejection")
	}

	if !strings.Contains(err.Error(), "outside its worktree") {
		t.Fatalf("LoadSchedulerSources() error = %q, want worktree escape message", err)
	}
}

func TestLoadSchedulerSourcesValidatesLimits(t *testing.T) {
	worktree := t.TempDir()

	tests := []struct {
		name         string
		perFileLimit int
		totalLimit   int
	}{
		{name: "per file", perFileLimit: 0, totalLimit: 1},
		{name: "total", perFileLimit: 1, totalLimit: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := LoadSchedulerSources(worktree, test.perFileLimit, test.totalLimit); err == nil {
				t.Fatal("LoadSchedulerSources() error = nil, want invalid limit error")
			}
		})
	}
}

func writeSchedulerSource(t *testing.T, worktree, path, content string) {
	t.Helper()

	absolute := filepath.Join(worktree, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		t.Fatalf("create source directory: %v", err)
	}

	if err := os.WriteFile(absolute, []byte(content), 0o644); err != nil {
		t.Fatalf("write scheduler source: %v", err)
	}
}
