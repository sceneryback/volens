package source

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type testRepository struct {
	remote string
	seed   string
	repo   string
}

func TestManagerListBranches(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "true")

	repository := newTestRepository(
		t,
		"master",
		[]string{"feature/nested", "release-1.0", "dependabot/go_modules", "main"},
	)
	manager := NewManager(repository.repo, repository.remote)

	branches, err := manager.ListBranches(context.Background())
	if err != nil {
		t.Fatalf("ListBranches() error = %v", err)
	}

	want := []string{
		"master",
		"main",
		"dependabot/go_modules",
		"feature/nested",
		"release-1.0",
	}

	if !reflect.DeepEqual(branches, want) {
		t.Fatalf("ListBranches() = %v, want %v", branches, want)
	}
}

func TestManagerListBranchesUpdatesOriginURLAndRefs(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "true")

	original := newTestRepository(t, "master", []string{"old-only"})
	replacement := newTestRepository(t, "new-main", []string{"new-only"})
	manager := NewManager(original.repo, replacement.remote)

	branches, err := manager.ListBranches(context.Background())
	if err != nil {
		t.Fatalf("ListBranches() error = %v", err)
	}

	want := []string{"new-main", "new-only"}
	if !reflect.DeepEqual(branches, want) {
		t.Fatalf("ListBranches() = %v, want %v", branches, want)
	}

	remoteURL := testGitOutput(t, original.repo, "remote", "get-url", "origin")
	if remoteURL != replacement.remote {
		t.Fatalf("origin URL = %q, want %q", remoteURL, replacement.remote)
	}
}

func TestManagerRemoteSwitchReplacesAndPrunesSeedTags(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "true")

	original := newTestRepository(t, "master", nil)
	testGit(t, original.seed, "tag", "shared-tag")
	testGit(t, original.seed, "tag", "seed-only-tag")
	testGit(t, original.seed, "push", "origin", "--tags")
	testGit(t, original.repo, "fetch", "--tags", "origin")

	replacement := newTestRepository(t, "main", nil)
	testGit(t, replacement.seed, "tag", "shared-tag")
	testGit(t, replacement.seed, "push", "origin", "shared-tag")

	manager := NewManager(original.repo, replacement.remote)
	if _, err := manager.ListBranches(context.Background()); err != nil {
		t.Fatalf("ListBranches() error = %v", err)
	}

	gotShared := testGitOutput(t, original.repo, "rev-parse", "shared-tag^{commit}")
	wantShared := testGitOutput(
		t,
		"",
		"--git-dir",
		replacement.remote,
		"rev-parse",
		"shared-tag^{commit}",
	)
	if gotShared != wantShared {
		t.Fatalf("shared tag commit = %q, want replacement commit %q", gotShared, wantShared)
	}

	if found, err := refExists(
		context.Background(),
		original.repo,
		"refs/tags/seed-only-tag",
	); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("seed-only tag was not pruned after switching origin")
	}
}

func TestManagerPrepareBranchCreatesAndUpdatesDetachedWorktree(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "true")

	repository := newTestRepository(t, "master", []string{"release-1.0"})
	manager := NewManager(repository.repo, repository.remote)
	ctx := context.Background()

	worktree, err := manager.PrepareBranch(ctx, "release-1.0")
	if err != nil {
		t.Fatalf("PrepareBranch() error = %v", err)
	}

	firstHead := testGitOutput(t, worktree, "rev-parse", "HEAD")
	wantFirstHead := testGitOutput(
		t,
		repository.repo,
		"rev-parse",
		"refs/remotes/origin/release-1.0",
	)
	if firstHead != wantFirstHead {
		t.Fatalf("worktree HEAD = %q, want %q", firstHead, wantFirstHead)
	}

	commitToBranch(t, repository, "release-1.0", "updated release")

	updatedWorktree, err := manager.PrepareBranch(ctx, "release-1.0")
	if err != nil {
		t.Fatalf("second PrepareBranch() error = %v", err)
	}

	if updatedWorktree == worktree {
		t.Fatalf("updated worktree = %q, want a commit-isolated path", updatedWorktree)
	}

	updatedHead := testGitOutput(t, updatedWorktree, "rev-parse", "HEAD")
	if updatedHead == firstHead {
		t.Fatal("PrepareBranch() did not move the worktree to the fetched branch head")
	}

	branchName := testGitOutput(t, updatedWorktree, "rev-parse", "--abbrev-ref", "HEAD")
	if branchName != "HEAD" {
		t.Fatalf("worktree branch = %q, want detached HEAD", branchName)
	}
}

func TestManagerPrepareBranchDoesNotFallback(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "true")

	repository := newTestRepository(t, "master", nil)
	manager := NewManager(repository.repo, repository.remote)

	worktree, err := manager.PrepareBranch(context.Background(), "does-not-exist")
	if err == nil {
		t.Fatal("PrepareBranch() error = nil, want missing branch error")
	}

	if worktree != "" {
		t.Fatalf("PrepareBranch() worktree = %q, want empty", worktree)
	}

	if !strings.Contains(err.Error(), `remote branch "does-not-exist" does not exist`) {
		t.Fatalf("PrepareBranch() error = %q, want explicit missing branch error", err)
	}
}

func TestManagerPrepareBranchRejectsInvalidBranch(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "false")

	repository := newTestRepository(t, "master", nil)
	manager := NewManager(repository.repo, repository.remote)

	invalidBranches := []string{
		"",
		" master",
		"master ",
		"HEAD",
		"../master",
		"master^{commit}",
	}

	for _, branch := range invalidBranches {
		t.Run(fmt.Sprintf("branch_%q", branch), func(t *testing.T) {
			if _, err := manager.PrepareBranch(context.Background(), branch); err == nil {
				t.Fatalf("PrepareBranch(%q) error = nil, want validation error", branch)
			}
		})
	}
}

func TestManagerPrepareRetainsTagResolutionAndMasterFallback(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "true")

	repository := newTestRepository(t, "master", []string{"release-1.0"})
	testGit(t, repository.seed, "checkout", "release-1.0")
	testGit(t, repository.seed, "tag", "v1.0")
	testGit(t, repository.seed, "push", "origin", "v1.0")
	manager := NewManager(repository.repo, repository.remote)

	tagWorktree, err := manager.Prepare(context.Background(), "1.0")
	if err != nil {
		t.Fatalf("Prepare(tag) error = %v", err)
	}

	tagHead := testGitOutput(t, tagWorktree, "rev-parse", "HEAD")
	wantTagHead := testGitOutput(t, repository.repo, "rev-parse", "v1.0")
	if tagHead != wantTagHead {
		t.Fatalf("tag worktree HEAD = %q, want %q", tagHead, wantTagHead)
	}

	fallbackWorktree, err := manager.Prepare(context.Background(), "unknown-version")
	if err != nil {
		t.Fatalf("Prepare(fallback) error = %v", err)
	}

	fallbackHead := testGitOutput(t, fallbackWorktree, "rev-parse", "HEAD")
	wantFallbackHead := testGitOutput(
		t,
		repository.repo,
		"rev-parse",
		"refs/remotes/origin/master",
	)
	if fallbackHead != wantFallbackHead {
		t.Fatalf("fallback worktree HEAD = %q, want %q", fallbackHead, wantFallbackHead)
	}

	emptyTagWorktree, err := manager.Prepare(context.Background(), "")
	if err != nil {
		t.Fatalf("Prepare(empty tag) error = %v", err)
	}

	if emptyTagWorktree != fallbackWorktree {
		t.Fatalf(
			"empty-tag fallback worktree = %q, want commit-specific master worktree %q",
			emptyTagWorktree,
			fallbackWorktree,
		)
	}
}

func TestRecommendBranchFromSchedulerVersion(t *testing.T) {
	branches := []string{"master", "main", "release-1.9", "release-1.10"}

	if branch := RecommendBranch("v1.9.0", branches); branch != "release-1.9" {
		t.Fatalf("branch=%q", branch)
	}

	if branch := RecommendBranch("v1.11.0", branches); branch != "master" {
		t.Fatalf("fallback branch=%q", branch)
	}
}

func TestManagerPrepareUsesCommitSpecificWorktreeForMovingFallback(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "true")

	repository := newTestRepository(t, "master", nil)
	manager := NewManager(repository.repo, repository.remote)
	ctx := context.Background()

	firstWorktree, err := manager.Prepare(ctx, "unknown-version")
	if err != nil {
		t.Fatalf("Prepare(first fallback) error = %v", err)
	}

	firstHead := testGitOutput(t, firstWorktree, "rev-parse", "HEAD")
	commitToBranch(t, repository, "master", "updated master")

	secondWorktree, err := manager.Prepare(ctx, "unknown-version")
	if err != nil {
		t.Fatalf("Prepare(second fallback) error = %v", err)
	}

	if secondWorktree == firstWorktree {
		t.Fatalf("moving fallback reused worktree %q", firstWorktree)
	}

	if head := testGitOutput(t, firstWorktree, "rev-parse", "HEAD"); head != firstHead {
		t.Fatalf("first worktree HEAD changed from %q to %q", firstHead, head)
	}

	if head := testGitOutput(t, secondWorktree, "rev-parse", "HEAD"); head == firstHead {
		t.Fatalf("second worktree stayed at old HEAD %q", head)
	}
}

func TestManagerPrepareUsesFetchedRemoteBranchInsteadOfSeedLocalBranch(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "true")

	original := newTestRepository(t, "master", nil)
	replacement := newTestRepository(t, "master", nil)
	commitToBranch(t, replacement, "master", "replacement master")
	manager := NewManager(original.repo, replacement.remote)

	worktree, err := manager.Prepare(context.Background(), "master")
	if err != nil {
		t.Fatalf("Prepare(master) error = %v", err)
	}

	got := testGitOutput(t, worktree, "rev-parse", "HEAD")
	want := testGitOutput(
		t,
		"",
		"--git-dir",
		replacement.remote,
		"rev-parse",
		"refs/heads/master",
	)
	if got != want {
		t.Fatalf("worktree HEAD = %q, want fetched replacement master %q", got, want)
	}

	seed := testGitOutput(t, original.repo, "rev-parse", "refs/heads/master")
	if got == seed {
		t.Fatalf("worktree unexpectedly used stale seed local branch %q", seed)
	}
}

func TestManagerSerializesConcurrentBranchPreparation(t *testing.T) {
	t.Setenv("VOLCANO_GIT_UPDATE", "false")

	repository := newTestRepository(t, "master", []string{"release-1.0"})
	manager := NewManager(repository.repo, repository.remote)

	const workers = 4

	var waitGroup sync.WaitGroup
	results := make(chan string, workers)
	errors := make(chan error, workers)

	for range workers {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			worktree, err := manager.PrepareBranch(context.Background(), "release-1.0")
			if err != nil {
				errors <- err

				return
			}

			results <- worktree
		}()
	}

	waitGroup.Wait()
	close(results)
	close(errors)

	for err := range errors {
		t.Errorf("concurrent PrepareBranch() error = %v", err)
	}

	var want string

	for worktree := range results {
		if want == "" {
			want = worktree
		}

		if worktree != want {
			t.Errorf("concurrent worktree = %q, want %q", worktree, want)
		}
	}
}

func newTestRepository(t *testing.T, baseBranch string, branches []string) testRepository {
	t.Helper()

	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	seed := filepath.Join(root, "seed")
	repo := filepath.Join(root, "repo")

	testGit(t, "", "init", "--bare", remote)
	testGit(t, "", "init", "--initial-branch="+baseBranch, seed)
	testGit(t, seed, "config", "user.name", "Volens Test")
	testGit(t, seed, "config", "user.email", "volens@example.invalid")

	writeTestFile(t, filepath.Join(seed, "version.txt"), baseBranch)
	testGit(t, seed, "add", "version.txt")
	testGit(t, seed, "commit", "-m", "initial commit")

	for index, branch := range branches {
		testGit(t, seed, "checkout", "-b", branch, baseBranch)
		writeTestFile(
			t,
			filepath.Join(seed, "version.txt"),
			fmt.Sprintf("%s-%d", branch, index),
		)
		testGit(t, seed, "add", "version.txt")
		testGit(t, seed, "commit", "-m", "add "+branch)
	}

	testGit(t, seed, "checkout", baseBranch)
	testGit(t, seed, "remote", "add", "origin", remote)
	testGit(t, seed, "push", "--all", "origin")
	testGit(t, "", "--git-dir", remote, "symbolic-ref", "HEAD", "refs/heads/"+baseBranch)
	testGit(t, "", "clone", remote, repo)

	return testRepository{
		remote: remote,
		seed:   seed,
		repo:   repo,
	}
}

func commitToBranch(t *testing.T, repository testRepository, branch, contents string) {
	t.Helper()

	testGit(t, repository.seed, "checkout", branch)
	writeTestFile(t, filepath.Join(repository.seed, "version.txt"), contents)
	testGit(t, repository.seed, "add", "version.txt")
	testGit(t, repository.seed, "commit", "-m", "update "+branch)
	testGit(t, repository.seed, "push", "origin", branch)
}

func writeTestFile(t *testing.T, path, contents string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func testGit(t *testing.T, dir string, args ...string) {
	t.Helper()

	command := exec.Command("git", args...)
	command.Dir = dir

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
}

func testGitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()

	command := exec.Command("git", args...)
	command.Dir = dir

	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}

	return strings.TrimSpace(string(output))
}
