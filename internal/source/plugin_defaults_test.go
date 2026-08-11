package source

import (
	"reflect"
	"strings"
	"testing"
)

func TestLoadSchedulerPluginDefaults(t *testing.T) {
	worktree := t.TempDir()

	writeSchedulerSource(t, worktree, schedulerPluginOptionsPath, `package conf

type PluginOption struct {
	Name             string                 `+"`yaml:\"name\"`"+`
	EnabledPredicate *bool                  `+"`yaml:\"enablePredicate\"`"+`
	EnabledEnqueue   *bool                  `+"`yaml:\"enableJobEnqueued,omitempty\"`"+`
	Arguments        map[string]interface{} `+"`yaml:\"arguments\"`"+`
}
`)
	writeSchedulerSource(t, worktree, schedulerPluginDefaultsPath, `package plugins

import "example.invalid/volcano/pkg/scheduler/conf"

func ApplyPluginConfDefaults(option *conf.PluginOption) {
	enabled := true
	disabled := false

	if option.EnabledPredicate == nil {
		option.EnabledPredicate = &enabled
	}

	if nil == option.EnabledEnqueue {
		option.EnabledEnqueue = &disabled
	}
}
`)

	defaults, err := LoadSchedulerPluginDefaults(worktree)
	if err != nil {
		t.Fatalf("LoadSchedulerPluginDefaults() error = %v", err)
	}

	want := map[string]bool{
		"enablePredicate":   true,
		"enableJobEnqueued": false,
	}
	if !reflect.DeepEqual(defaults, want) {
		t.Fatalf("LoadSchedulerPluginDefaults() = %#v, want %#v", defaults, want)
	}
}

func TestLoadSchedulerPluginDefaultsRejectsMissingSource(t *testing.T) {
	worktree := t.TempDir()

	writeSchedulerSource(t, worktree, schedulerPluginOptionsPath, `package conf

type PluginOption struct {
	EnabledPredicate *bool `+"`yaml:\"enablePredicate\"`"+`
}
`)

	_, err := LoadSchedulerPluginDefaults(worktree)
	if err == nil {
		t.Fatal("LoadSchedulerPluginDefaults() error = nil, want missing defaults source error")
	}

	if !strings.Contains(err.Error(), schedulerPluginDefaultsPath) || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("LoadSchedulerPluginDefaults() error = %q, want missing source path", err)
	}
}

func TestLoadSchedulerPluginDefaultsRejectsUnsupportedSyntax(t *testing.T) {
	worktree := t.TempDir()

	writeSchedulerSource(t, worktree, schedulerPluginOptionsPath, `package conf

type PluginOption struct {
	EnabledPredicate *bool `+"`yaml:\"enablePredicate\"`"+`
}
`)
	writeSchedulerSource(t, worktree, schedulerPluginDefaultsPath, `package plugins

import "example.invalid/volcano/pkg/scheduler/conf"

func ApplyPluginConfDefaults(option *conf.PluginOption) {
	if option.EnabledPredicate == nil {
		option.EnabledPredicate = boolPointer(true)
	}
}
`)

	_, err := LoadSchedulerPluginDefaults(worktree)
	if err == nil {
		t.Fatal("LoadSchedulerPluginDefaults() error = nil, want unsupported syntax error")
	}

	if !strings.Contains(err.Error(), "unsupported default syntax") ||
		!strings.Contains(err.Error(), "PluginOption.EnabledPredicate") {
		t.Fatalf("LoadSchedulerPluginDefaults() error = %q, want field-specific syntax error", err)
	}
}

func TestLoadSchedulerPluginDefaultsRejectsMissingDefaultFunction(t *testing.T) {
	worktree := t.TempDir()

	writeSchedulerSource(t, worktree, schedulerPluginOptionsPath, `package conf

type PluginOption struct {
	EnabledPredicate *bool `+"`yaml:\"enablePredicate\"`"+`
}
`)
	writeSchedulerSource(t, worktree, schedulerPluginDefaultsPath, `package plugins

func anotherFunction() {}
`)

	_, err := LoadSchedulerPluginDefaults(worktree)
	if err == nil {
		t.Fatal("LoadSchedulerPluginDefaults() error = nil, want missing function error")
	}

	if !strings.Contains(err.Error(), "ApplyPluginConfDefaults") || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("LoadSchedulerPluginDefaults() error = %q, want missing function message", err)
	}
}
