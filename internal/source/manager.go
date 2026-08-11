package source

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

const originRemote = "origin"

var unsafeWorktreeCharacter = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

type Manager struct {
	dir    string
	remote string
	mu     sync.Mutex
}

func NewManager(dir, remote string) *Manager {
	return &Manager{
		dir:    dir,
		remote: remote,
	}
}

// Prepare keeps the tag-oriented source lookup used by automatic scheduler
// detection. Its fallback behavior remains compatible with the original
// implementation.
func (m *Manager) Prepare(ctx context.Context, tag string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.prepareRepository(ctx); err != nil {
		return "", err
	}

	ref := resolveRef(ctx, m.dir, tag)
	commit, err := gitOutput(ctx, m.dir, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve Volcano source ref %q: %w", ref, err)
	}

	work := filepath.Join(
		filepath.Dir(m.dir),
		"worktrees",
		branchWorktreeName("auto-"+ref, commit),
	)

	return prepareWorktree(ctx, m.dir, work, commit)
}

// ListBranches returns branch names from the configured remote. The names do
// not include the origin/ prefix and can be passed directly to PrepareBranch.
func (m *Manager) ListBranches(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.prepareRepository(ctx); err != nil {
		return nil, err
	}

	output, err := gitOutput(
		ctx,
		m.dir,
		"for-each-ref",
		"--sort=refname",
		"--format=%(refname:strip=3)",
		"refs/remotes/origin",
	)
	if err != nil {
		return nil, err
	}

	branches := make([]string, 0)

	for _, branch := range strings.Split(output, "\n") {
		branch = strings.TrimSpace(branch)

		if branch == "" || branch == "HEAD" {
			continue
		}

		branches = append(branches, branch)
	}

	sort.Slice(branches, func(left, right int) bool {
		leftPriority := branchPriority(branches[left])
		rightPriority := branchPriority(branches[right])

		if leftPriority != rightPriority {
			return leftPriority < rightPriority
		}

		return branches[left] < branches[right]
	})

	return branches, nil
}

// PrepareBranch creates or updates a detached worktree at the exact branch on
// the configured remote. It deliberately does not fall back to another ref.
func (m *Manager) PrepareBranch(ctx context.Context, branch string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := validateBranch(ctx, branch); err != nil {
		return "", err
	}

	if err := m.prepareRepository(ctx); err != nil {
		return "", err
	}

	ref := "refs/remotes/origin/" + branch
	exists, err := refExists(ctx, m.dir, ref)
	if err != nil {
		return "", err
	}

	if !exists {
		return "", fmt.Errorf("remote branch %q does not exist", branch)
	}

	commit, err := gitOutput(ctx, m.dir, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve remote branch %q: %w", branch, err)
	}

	work := filepath.Join(
		filepath.Dir(m.dir),
		"worktrees",
		branchWorktreeName(branch, commit),
	)

	return prepareWorktree(ctx, m.dir, work, commit)
}

func (m *Manager) prepareRepository(ctx context.Context) error {
	if _, err := os.Stat(filepath.Join(m.dir, ".git")); err != nil {
		return fmt.Errorf(
			"volcano source missing at %s (image entrypoint must clone it): %w",
			m.dir,
			err,
		)
	}

	if strings.TrimSpace(m.remote) == "" {
		return errors.New("volcano remote URL is empty")
	}

	if err := configureOrigin(ctx, m.dir, m.remote); err != nil {
		return err
	}

	if os.Getenv("VOLCANO_GIT_UPDATE") == "false" {
		return nil
	}

	return runGit(
		ctx,
		m.dir,
		"fetch",
		"--prune",
		"--prune-tags",
		originRemote,
		"+refs/heads/*:refs/remotes/origin/*",
		"+refs/tags/*:refs/tags/*",
	)
}

func configureOrigin(ctx context.Context, dir, remote string) error {
	// set remote url
	if err := runGit(
		ctx,
		dir,
		"config",
		"--local",
		"--replace-all",
		"remote.origin.url",
		remote,
	); err != nil {
		return fmt.Errorf("configure origin URL: %w", err)
	}

	// set fetch policy
	if err := runGit(
		ctx,
		dir,
		"config",
		"--local",
		"--replace-all",
		"remote.origin.fetch",
		"+refs/heads/*:refs/remotes/origin/*",
	); err != nil {
		return fmt.Errorf("configure origin fetch refspec: %w", err)
	}

	return nil
}

func validateBranch(ctx context.Context, branch string) error {
	if branch == "" || branch != strings.TrimSpace(branch) {
		return errors.New("volcano branch is empty or contains surrounding whitespace")
	}

	if branch == "HEAD" {
		return errors.New(`volcano branch "HEAD" is not an explicit remote branch`)
	}

	if err := runGit(ctx, "", "check-ref-format", "--branch", branch); err != nil {
		return fmt.Errorf("invalid volcano branch %q: %w", branch, err)
	}

	return nil
}

func prepareWorktree(ctx context.Context, repository, work, ref string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(work), 0o755); err != nil {
		return "", fmt.Errorf("create worktree parent: %w", err)
	}

	_, err := os.Stat(work)

	switch {
	case os.IsNotExist(err):
		if err := runGit(
			ctx,
			repository,
			"worktree",
			"add",
			"--detach",
			work,
			ref,
		); err != nil {
			return "", err
		}
	case err != nil:
		return "", fmt.Errorf("inspect worktree %s: %w", work, err)
	default:
		if err := runGit(ctx, work, "checkout", "--detach", "--force", ref); err != nil {
			return "", err
		}
	}

	return work, nil
}

func branchPriority(branch string) int {
	switch branch {
	case "master":
		return 0
	case "main":
		return 1
	default:
		return 2
	}
}

// RecommendBranch chooses the best branch name for a scheduler semantic
// version. It prefers exact release branches, then the default branch.
func RecommendBranch(version string, branches []string) string {
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	if version == "" {
		return firstDefaultBranch(branches)
	}

	parts := strings.Split(version, ".")
	candidates := []string{
		"release-" + version,
		"v" + version,
		version,
	}

	if len(parts) >= 2 {
		majorMinor := parts[0] + "." + parts[1]
		candidates = append(candidates, "release-"+majorMinor, "v"+majorMinor, majorMinor)
	}

	for _, candidate := range candidates {
		for _, branch := range branches {
			if branch == candidate {
				return branch
			}
		}
	}

	return firstDefaultBranch(branches)
}

func firstDefaultBranch(branches []string) string {
	for _, wanted := range []string{"master", "main"} {
		for _, branch := range branches {
			if branch == wanted {
				return branch
			}
		}
	}

	if len(branches) > 0 {
		return branches[0]
	}

	return ""
}

func branchWorktreeName(branch, commit string) string {
	safe := unsafeWorktreeCharacter.ReplaceAllString(branch, "_")
	if len(safe) > 80 {
		safe = safe[:80]
	}

	sum := sha256.Sum256([]byte(branch))
	shortCommit := commit
	if len(shortCommit) > 12 {
		shortCommit = shortCommit[:12]
	}

	return fmt.Sprintf("branch-%s-%x-%s", safe, sum[:6], shortCommit)
}

func resolveRef(ctx context.Context, dir, tag string) string {
	tag = strings.TrimSpace(tag)
	candidates := make([]string, 0, 4)

	if tag != "" {
		candidates = append(candidates, "refs/tags/"+tag)

		versionTag := "v" + strings.TrimPrefix(tag, "v")
		if versionTag != tag {
			candidates = append(candidates, "refs/tags/"+versionTag)
		}

		candidates = append(
			candidates,
			"refs/remotes/origin/"+tag,
			"refs/remotes/origin/release-"+strings.TrimPrefix(tag, "v"),
		)
	}

	for _, ref := range candidates {
		if refExists, _ := refExists(ctx, dir, ref+"^{commit}"); refExists {
			return ref
		}
	}

	return "refs/remotes/origin/master"
}

func refExists(ctx context.Context, dir, ref string) (bool, error) {
	command := exec.CommandContext(ctx, "git", "rev-parse", "--verify", "--quiet", ref)
	command.Dir = dir

	err := command.Run()
	if err == nil {
		return true, nil
	}

	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	var exitError *exec.ExitError
	if errors.As(err, &exitError) && exitError.ExitCode() == 1 {
		return false, nil
	}

	return false, fmt.Errorf("verify git ref: %w", err)
}

func runGit(ctx context.Context, dir string, args ...string) error {
	_, err := gitOutput(ctx, dir, args...)

	return err
}

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "git", args...)
	command.Dir = dir

	output, err := command.CombinedOutput()
	if err != nil {
		operation := "command"
		if len(args) > 0 {
			operation = args[0]
		}

		return "", fmt.Errorf(
			"git %s: %w: %s",
			operation,
			err,
			strings.TrimSpace(string(output)),
		)
	}

	return strings.TrimSpace(string(output)), nil
}
