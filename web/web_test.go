package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIndex(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}

	for _, expected := range []string{
		"Volens",
		`id="pod"`,
		`id="branch"`,
		"Volcano branch",
		`id="language"`,
		`class="logo"`,
	} {
		if !strings.Contains(rec.Body.String(), expected) {
			t.Fatalf("index page does not contain %q", expected)
		}
	}
}

func TestAppRequestsBranchesAndSubmitsSelection(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/app.js", nil)
	rec := httptest.NewRecorder()

	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}

	for _, expected := range []string{
		`fetch("/api/branches")`,
		`JSON.stringify({ namespace, pod, branch })`,
		`renderEnqueue(report.enqueue || {})`,
		`renderJobValid(report.jobValid || {})`,
		`renderAllocate(report.allocate || {})`,
		`clearReport();`,
		`startProgress();`,
		`progressStages`,
		`free / total`,
		`statusMark("skipped")`,
	} {
		if !strings.Contains(rec.Body.String(), expected) {
			t.Fatalf("app.js does not contain %q", expected)
		}
	}
}
