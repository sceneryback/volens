package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/volcano-sh/volens/internal/agent"
	"github.com/volcano-sh/volens/internal/source"
)

func TestGinRouterServesHealthStaticAndBranches(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sourceManager := newTestSourceManager(t)
	analysisAgent := agent.New(nil, sourceManager, agent.LLMConfig{})
	router := newRouter(nil, sourceManager, analysisAgent)

	for _, test := range []struct {
		path     string
		contains string
	}{
		{
			path:     "/healthz",
			contains: "ok",
		},
		{
			path:     "/",
			contains: "Volens Debug Agent",
		},
		{
			path:     "/api/branches",
			contains: "release-1.12",
		},
	} {
		recorder := httptest.NewRecorder()

		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))

		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), test.contains) {
			t.Errorf("GET %s: status=%d body=%q", test.path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestDecodeAnalyzeRequestPreservesOptionalBranch(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, test := range []struct {
		name   string
		body   string
		branch string
	}{
		{
			name:   "explicit branch",
			body:   `{"namespace":"default","pod":"pod-a","branch":"release-1.12"}`,
			branch: "release-1.12",
		},
		{
			name: "automatic tag compatibility",
			body: `{"namespace":"default","pod":"pod-a"}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(recorder)
			context.Request = httptest.NewRequest(
				http.MethodPost,
				"/api/analyze",
				strings.NewReader(test.body),
			)

			var request agent.Request

			if err := decodeAnalyzeRequest(context, &request); err != nil {
				t.Fatal(err)
			}

			if request.Namespace != "default" || request.Pod != "pod-a" {
				t.Fatalf("request=%+v", request)
			}

			if request.Branch != test.branch {
				t.Fatalf("branch=%q", request.Branch)
			}
		})
	}
}

func TestGinAPIErrorResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sourceManager := source.NewManager(t.TempDir(), "unused")
	analysisAgent := agent.New(nil, sourceManager, agent.LLMConfig{})
	router := newRouter(nil, sourceManager, analysisAgent)
	oversizedBody := `{"namespace":"` +
		strings.Repeat("x", int(maxAnalyzeBodyBytes)) +
		`","pod":"p"}`

	tests := []struct {
		method string
		path   string
		body   string
		status int
	}{
		{
			method: http.MethodGet,
			path:   "/api/branches",
			status: http.StatusInternalServerError,
		},
		{
			method: http.MethodPost,
			path:   "/api/analyze",
			body:   "{",
			status: http.StatusBadRequest,
		},
		{
			method: http.MethodPost,
			path:   "/api/analyze",
			body:   `{"namespace":"default"}`,
			status: http.StatusBadRequest,
		},
		{
			method: http.MethodGet,
			path:   "/api/analyze",
			status: http.StatusMethodNotAllowed,
		},
		{
			method: http.MethodGet,
			path:   "/api/missing",
			status: http.StatusNotFound,
		},
		{
			method: http.MethodPost,
			path:   "/api/analyze",
			body:   oversizedBody,
			status: http.StatusRequestEntityTooLarge,
		},
	}

	for _, test := range tests {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))

		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, request)

		if recorder.Code != test.status {
			t.Errorf(
				"%s %s: status=%d want=%d body=%s",
				test.method,
				test.path,
				recorder.Code,
				test.status,
				recorder.Body.String(),
			)
		}

		if !strings.Contains(recorder.Header().Get("Content-Type"), "application/json") {
			t.Errorf("%s %s content-type=%q", test.method, test.path, recorder.Header().Get("Content-Type"))
		}
	}
}

func newTestSourceManager(t *testing.T) *source.Manager {
	t.Helper()
	t.Setenv("VOLCANO_GIT_UPDATE", "false")

	repository := filepath.Join(t.TempDir(), "volcano")
	runTestGit(t, "", "init", "--initial-branch=master", repository)
	runTestGit(t, repository, "config", "user.name", "Volens Test")
	runTestGit(t, repository, "config", "user.email", "volens@example.invalid")

	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}

	runTestGit(t, repository, "add", "README.md")
	runTestGit(t, repository, "commit", "-m", "initial commit")
	runTestGit(t, repository, "update-ref", "refs/remotes/origin/master", "HEAD")
	runTestGit(t, repository, "update-ref", "refs/remotes/origin/release-1.12", "HEAD")

	return source.NewManager(repository, "https://example.invalid/volcano.git")
}

func runTestGit(t *testing.T, dir string, args ...string) {
	t.Helper()

	command := exec.Command("git", args...)
	command.Dir = dir

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
}
