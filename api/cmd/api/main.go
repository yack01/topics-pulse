// Command api is the TopicsPulse HTTP server. It also owns two background
// jobs in the same process (no extra containers): the embedding worker and
// the nightly topic-analysis cron.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"

	"topicspulse/internal/config"
	"topicspulse/internal/db"
	"topicspulse/internal/handlers"
	"topicspulse/internal/ollama"
	"topicspulse/internal/repository"
	"topicspulse/internal/service"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	pool, err := db.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	llmClient := ollama.NewClient(cfg.OllamaBaseURL, cfg.OllamaEmbedModel, cfg.OllamaLLMModel, cfg.OllamaKeepAlive, cfg.OllamaRequestTimeout)

	messagesRepo := repository.NewMessagesRepo(pool)
	embeddingsRepo := repository.NewEmbeddingsRepo(pool)
	topicsRepo := repository.NewTopicsRepo(pool)
	searchRepo := repository.NewSearchRepo(pool)

	ingestSvc := service.NewIngestService(messagesRepo)
	analysisSvc := service.NewAnalysisService(cfg, messagesRepo, embeddingsRepo, topicsRepo, llmClient)
	userTopicsSvc := service.NewUserTopicsService(cfg, messagesRepo, llmClient)
	searchSvc := service.NewSearchService(cfg, searchRepo)

	// Background embedding worker.
	embeddingWorker := service.NewEmbeddingWorker(messagesRepo, embeddingsRepo, llmClient, cfg.EmbeddingPollInterval, cfg.EmbeddingBatchSize, cfg.OllamaEmbedModel)
	go embeddingWorker.Run(ctx)

	// Nightly analysis cron.
	c := cron.New()
	if _, err := c.AddFunc(cfg.AnalysisCronExpr, func() {
		log.Println("nightly analysis: starting")
		analysisSvc.RunNightly(ctx)
		log.Println("nightly analysis: finished")
	}); err != nil {
		log.Fatalf("invalid ANALYSIS_CRON expression %q: %v", cfg.AnalysisCronExpr, err)
	}
	c.Start()
	defer c.Stop()

	mux := http.NewServeMux()
	h := handlers.New(cfg, ingestSvc, analysisSvc, userTopicsSvc, searchSvc)
	h.Register(mux)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handlers.CORS(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("TopicsPulse API listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown: %v", err)
	}
}
