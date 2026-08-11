package agent

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestBuildPromptIndexesSelectedWorktreeHooks(t *testing.T) {
	worktree := t.TempDir()
	files := map[string]string{
		"pkg/scheduler/actions/allocate/allocate.go": "package allocate\n",
		"pkg/scheduler/plugins/custom/register.go": `package custom

func register(session Session) {
	session.AddJobEnqueueableFn("custom", enqueue)
}
`,
		"pkg/scheduler/plugins/custom/logic.go": "package custom\n\nfunc enqueue() {}\n",
	}

	for path, content := range files {
		absolute := filepath.Join(worktree, filepath.FromSlash(path))

		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			t.Fatal(err)
		}

		if err := os.WriteFile(absolute, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	prompt, paths, err := buildPrompt(Report{}, worktree)
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{
		"pkg/scheduler/actions/allocate/allocate.go",
		"pkg/scheduler/plugins/custom/logic.go",
		"pkg/scheduler/plugins/custom/register.go",
	} {
		if !slices.Contains(paths, path) {
			t.Errorf("source paths %v missing %s", paths, path)
		}

		if !strings.Contains(prompt, path) {
			t.Errorf("prompt missing %s", path)
		}
	}

	if !strings.Contains(prompt, "AddJobEnqueueableFn") || !strings.Contains(prompt, "func enqueue") {
		t.Fatalf("prompt missing discovered hook logic:\n%s", prompt)
	}
}
