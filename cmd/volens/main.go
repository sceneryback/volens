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
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/volcano-sh/volens/internal/agent"
	"github.com/volcano-sh/volens/internal/cluster"
	"github.com/volcano-sh/volens/internal/source"
	"github.com/volcano-sh/volens/web"
)

const maxAnalyzeBodyBytes int64 = 1 << 20

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

	gin.SetMode(env("GIN_MODE", gin.ReleaseMode))

	server := &http.Server{
		Addr:              env("VOLENS_ADDR", ":8080"),
		Handler:           newRouter(clusterManager, sourceManager, analysisAgent),
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
) *gin.Engine {
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
		value, err := sourceManager.ListBranches(c.Request.Context())
		if err != nil {
			writeError(c, http.StatusInternalServerError, err)

			return
		}

		c.JSON(http.StatusOK, gin.H{"branches": value})
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

		report, err := analysisAgent.Analyze(c.Request.Context(), request)
		if err != nil {
			writeError(c, http.StatusInternalServerError, err)

			return
		}

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
