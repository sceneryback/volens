package source

import (
	"reflect"
	"strings"
	"testing"
)

func TestLoadPredicatePluginDefaultsReturnsLinkedTrueAndFalseValues(t *testing.T) {
	worktree := t.TempDir()

	writeSchedulerSource(t, worktree, predicatePluginSourcePath, `package predicates

const (
	NodeAffinityEnable = "predicate.NodeAffinityEnable"
	CachePredicate = "predicate.CacheEnable"
	UnrelatedExpression = NodeAffinityEnable + ".suffix"
)

type predicateEnable struct {
	nodeAffinityEnable bool
	cacheEnable        bool
	proportional       map[string]int
}

func enablePredicate(args Arguments) predicateEnable {
	predicate := predicateEnable{
		nodeAffinityEnable: true,
		cacheEnable:        false,
	}

	args.GetBool(&predicate.nodeAffinityEnable, NodeAffinityEnable)
	args.GetBool(&predicate.cacheEnable, CachePredicate)
	predicate.proportional = map[string]int{}

	return predicate
}
`)

	defaults, err := LoadPredicatePluginDefaults(worktree)
	if err != nil {
		t.Fatalf("LoadPredicatePluginDefaults() error = %v", err)
	}

	want := map[string]bool{
		"predicate.NodeAffinityEnable": true,
		"predicate.CacheEnable":        false,
	}

	if !reflect.DeepEqual(defaults, want) {
		t.Fatalf("LoadPredicatePluginDefaults() = %#v, want %#v", defaults, want)
	}
}

func TestLoadPredicatePluginDefaultsRejectsMissingSource(t *testing.T) {
	_, err := LoadPredicatePluginDefaults(t.TempDir())
	if err == nil {
		t.Fatal("LoadPredicatePluginDefaults() error = nil, want missing source error")
	}

	if !strings.Contains(err.Error(), predicatePluginSourcePath) ||
		!strings.Contains(err.Error(), "missing") {
		t.Fatalf("LoadPredicatePluginDefaults() error = %q, want missing source path", err)
	}
}

func TestLoadPredicatePluginDefaultsRejectsMissingFunction(t *testing.T) {
	worktree := t.TempDir()

	writeSchedulerSource(t, worktree, predicatePluginSourcePath, `package predicates

type predicateEnable struct {
	nodeAffinityEnable bool
}
`)

	_, err := LoadPredicatePluginDefaults(worktree)
	if err == nil {
		t.Fatal("LoadPredicatePluginDefaults() error = nil, want missing function error")
	}

	if !strings.Contains(err.Error(), "enablePredicate") ||
		!strings.Contains(err.Error(), "missing") {
		t.Fatalf("LoadPredicatePluginDefaults() error = %q, want missing function message", err)
	}
}

func TestLoadPredicatePluginDefaultsRejectsUnsupportedAssociation(t *testing.T) {
	worktree := t.TempDir()

	writeSchedulerSource(t, worktree, predicatePluginSourcePath, `package predicates

const NodeAffinityEnable = "predicate.NodeAffinityEnable"

type predicateEnable struct {
	nodeAffinityEnable bool
}

func enablePredicate(args Arguments) predicateEnable {
	predicate := predicateEnable{
		nodeAffinityEnable: defaultNodeAffinity(),
	}

	args.GetBool(&predicate.nodeAffinityEnable, NodeAffinityEnable)

	return predicate
}
`)

	_, err := LoadPredicatePluginDefaults(worktree)
	if err == nil {
		t.Fatal("LoadPredicatePluginDefaults() error = nil, want unsupported syntax error")
	}

	if !strings.Contains(err.Error(), "unsupported bool default syntax") ||
		!strings.Contains(err.Error(), "nodeAffinityEnable") {
		t.Fatalf("LoadPredicatePluginDefaults() error = %q, want field-specific syntax error", err)
	}
}

func TestLoadPredicatePluginDefaultsRejectsNonConstantKey(t *testing.T) {
	worktree := t.TempDir()

	writeSchedulerSource(t, worktree, predicatePluginSourcePath, `package predicates

type predicateEnable struct {
	nodeAffinityEnable bool
}

func enablePredicate(args Arguments) predicateEnable {
	predicate := predicateEnable{
		nodeAffinityEnable: true,
	}

	args.GetBool(&predicate.nodeAffinityEnable, "predicate.NodeAffinityEnable")

	return predicate
}
`)

	_, err := LoadPredicatePluginDefaults(worktree)
	if err == nil {
		t.Fatal("LoadPredicatePluginDefaults() error = nil, want non-constant key error")
	}

	if !strings.Contains(err.Error(), "expected a constant identifier") {
		t.Fatalf("LoadPredicatePluginDefaults() error = %q, want constant identifier message", err)
	}
}
