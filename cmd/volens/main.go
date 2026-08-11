package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/volcano-sh/volens/internal/agent"
	"github.com/volcano-sh/volens/internal/cluster"
	"github.com/volcano-sh/volens/internal/source"
	"github.com/volcano-sh/volens/web"
)

const (
	maxAnalyzeBodyBytes   int64 = 1 << 20
	defaultAnalyzeTimeout       = 15 * time.Minute
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	clusterManager, err := cluster.NewInClusterClient()
	if err != nil {
		stop()

		return err
	}

	defer func() {
		stop()
		clusterManager.Shutdown()
	}()

	if err := clusterManager.Start(ctx); err != nil {
		return err
	}

	sourceManager := source.NewManager(
		env("VOLENS_SOURCE_DIR", "/var/lib/volens/volcano"),
		env("VOLCANO_REPO_URL", "https://github.com/volcano-sh/volcano.git"),
	)

	analysisAgent := agent.New(clusterManager, sourceManager, agent.LLMConfigFromEnv())
	schedulerVersionCache := newSchedulerVersionCache(ctx, clusterManager)

	gin.SetMode(env("GIN_MODE", gin.ReleaseMode))

	server := &http.Server{
		Addr:              env("VOLENS_ADDR", ":8080"),
		Handler:           newRouter(clusterManager, sourceManager, analysisAgent, schedulerVersionCache),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("volens listening on %s", server.Addr)

	return serve(ctx, server)
}

func newRouter(
	clusterManager *cluster.Client,
	sourceManager *source.Manager,
	analysisAgent *agent.Agent,
	schedulerVersionCache *schedulerVersionCache,
) *gin.Engine {
	analyzeSlots := make(chan struct{}, 1)
	router := gin.New()
	router.HandleMethodNotAllowed = true
	router.Use(
		gin.Logger(),
		gin.CustomRecovery(func(c *gin.Context, _ any) {
			c.AbortWithStatusJSON(
				http.StatusInternalServerError,
				gin.H{"error": "internal server error"},
			)
		}),
	)

	router.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	api := router.Group("/api")

	api.GET("/pods", func(c *gin.Context) {
		value, err := clusterManager.ListPendingPods(c.Request.Context())
		if err != nil {
			writeError(c, http.StatusInternalServerError, err)

			return
		}

		c.JSON(http.StatusOK, value)
	})

	api.GET("/branches", func(c *gin.Context) {
		branches, err := sourceManager.ListBranches(c.Request.Context())
		if err != nil {
			writeError(c, http.StatusInternalServerError, err)

			return
		}

		response := gin.H{"branches": branches}

		if schedulerVersionCache != nil {
			scheduler, versionErr, ready := schedulerVersionCache.snapshot()
			if ready && versionErr == nil {
				recommendedBranch := source.RecommendBranch(scheduler.Version, branches)
				response["schedulerVersion"] = scheduler
				response["recommendedBranch"] = recommendedBranch
				log.Printf(
					"detected Volcano scheduler version scheduler=%s/%s container=%s version=%s gitSHA=%s recommendedBranch=%s",
					scheduler.Namespace,
					scheduler.Name,
					scheduler.Container,
					scheduler.Version,
					scheduler.GitSHA,
					recommendedBranch,
				)
			} else if ready {
				recommendedBranch := source.RecommendBranch("", branches)
				response["schedulerVersionError"] = versionErr.Error()
				response["recommendedBranch"] = recommendedBranch
				log.Printf(
					"cached Volcano scheduler version detection failed err=%v recommendedBranch=%s",
					versionErr,
					recommendedBranch,
				)
			} else {
				recommendedBranch := source.RecommendBranch("", branches)
				response["schedulerVersionError"] = "scheduler version detection is still running"
				response["recommendedBranch"] = recommendedBranch
			}
		}

		c.JSON(http.StatusOK, response)
	})

	api.POST("/analyze", func(c *gin.Context) {
		var request agent.Request

		if err := decodeAnalyzeRequest(c, &request); err != nil {
			status := http.StatusBadRequest
			var maxBytesError *http.MaxBytesError

			if errors.As(err, &maxBytesError) {
				status = http.StatusRequestEntityTooLarge
			}

			writeError(c, status, err)

			return
		}

		if request.Namespace == "" || request.Pod == "" {
			writeError(c, http.StatusBadRequest, fmt.Errorf("namespace and pod are required"))

			return
		}

		started := time.Now()
		log.Printf(
			"received analyze request namespace=%s pod=%s branch=%s",
			request.Namespace,
			request.Pod,
			request.Branch,
		)

		analysisCtx, cancel := context.WithTimeout(
			c.Request.Context(),
			analyzeTimeoutFromEnv(),
		)
		defer cancel()

		select {
		case analyzeSlots <- struct{}{}:
			defer func() {
				<-analyzeSlots
			}()
		default:
			writeError(c, http.StatusTooManyRequests, fmt.Errorf("another analysis is already running"))

			return
		}

		report, err := analysisAgent.Analyze(analysisCtx, request)
		if err != nil {
			log.Printf(
				"analyze request failed namespace=%s pod=%s branch=%s duration=%s err=%v",
				request.Namespace,
				request.Pod,
				request.Branch,
				time.Since(started),
				err,
			)
			writeError(c, http.StatusInternalServerError, err)

			return
		}

		log.Printf(
			"analyze request completed namespace=%s pod=%s branch=%s duration=%s passed=%t conclusion=%q",
			request.Namespace,
			request.Pod,
			request.Branch,
			time.Since(started),
			report.Passed,
			report.Conclusion,
		)

		c.JSON(http.StatusOK, report)
	})

	router.NoMethod(func(c *gin.Context) {
		c.AbortWithStatusJSON(
			http.StatusMethodNotAllowed,
			gin.H{"error": "method not allowed"},
		)
	})

	staticHandler := web.Handler()

	router.NoRoute(func(c *gin.Context) {
		if c.Request.URL.Path == "/api" || strings.HasPrefix(c.Request.URL.Path, "/api/") {
			c.AbortWithStatusJSON(
				http.StatusNotFound,
				gin.H{"error": "not found"},
			)

			return
		}

		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.AbortWithStatusJSON(
				http.StatusNotFound,
				gin.H{"error": "not found"},
			)

			return
		}

		staticHandler.ServeHTTP(c.Writer, c.Request)
	})

	return router
}

func decodeAnalyzeRequest(c *gin.Context, destination *agent.Request) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxAnalyzeBodyBytes)
	decoder := json.NewDecoder(c.Request.Body)

	if err := decoder.Decode(destination); err != nil {
		return err
	}

	var extra any

	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("request body must contain one JSON object")
		}

		return err
	}

	return nil
}

func writeError(c *gin.Context, status int, err error) {
	c.AbortWithStatusJSON(status, gin.H{"error": err.Error()})
}

func serve(ctx context.Context, server *http.Server) error {
	result := make(chan error, 1)

	go func() {
		result <- server.ListenAndServe()
	}()

	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = server.Close()
	}

	serveErr := <-result
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}

	return shutdownErr
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func analyzeTimeoutFromEnv() time.Duration {
	value := strings.TrimSpace(os.Getenv("VOLENS_ANALYZE_TIMEOUT"))
	if value == "" {
		return defaultAnalyzeTimeout
	}

	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		return defaultAnalyzeTimeout
	}

	return timeout
}

type schedulerVersionCache struct {
	mu        sync.RWMutex
	ready     bool
	scheduler cluster.Scheduler
	err       error
}

func newSchedulerVersionCache(ctx context.Context, clusterManager *cluster.Client) *schedulerVersionCache {
	cache := &schedulerVersionCache{}

	if clusterManager == nil {
		cache.store(cluster.Scheduler{}, fmt.Errorf("cluster manager is not configured"))

		return cache
	}

	go func() {
		scheduler, err := clusterManager.GetVolcanoSchedulerVersion(ctx)
		cache.store(scheduler, err)

		if err != nil {
			log.Printf("startup Volcano scheduler version detection failed err=%v", err)

			return
		}

		log.Printf(
			"startup detected Volcano scheduler version scheduler=%s/%s container=%s version=%s gitSHA=%s",
			scheduler.Namespace,
			scheduler.Name,
			scheduler.Container,
			scheduler.Version,
			scheduler.GitSHA,
		)
	}()

	return cache
}

func (c *schedulerVersionCache) store(scheduler cluster.Scheduler, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ready = true
	c.scheduler = scheduler
	c.err = err
}

func (c *schedulerVersionCache) snapshot() (cluster.Scheduler, error, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.scheduler, c.err, c.ready
}
