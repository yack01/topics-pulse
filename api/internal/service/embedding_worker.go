package service

import (
	"context"
	"log"
	"time"

	"topicspulse/internal/ollama"
	"topicspulse/internal/repository"
)

// EmbeddingWorker polls for messages without an embedding and computes one
// via Ollama. It runs as a goroutine inside the API process (see main.go) —
// there is no separate embedding-worker container, per the "minimum
// infrastructure" goal. Decoupling it from POST /messages (rather than
// embedding synchronously on insert) keeps message ingestion fast and
// unaffected by model inference time or Ollama availability.
type EmbeddingWorker struct {
	messages   *repository.MessagesRepo
	embeddings *repository.EmbeddingsRepo
	ollama     *ollama.Client
	interval   time.Duration
	batchSize  int
	modelName  string
}

func NewEmbeddingWorker(messages *repository.MessagesRepo, embeddings *repository.EmbeddingsRepo, client *ollama.Client, interval time.Duration, batchSize int, modelName string) *EmbeddingWorker {
	return &EmbeddingWorker{
		messages:   messages,
		embeddings: embeddings,
		ollama:     client,
		interval:   interval,
		batchSize:  batchSize,
		modelName:  modelName,
	}
}

// Run blocks until ctx is cancelled, polling on a fixed interval.
func (w *EmbeddingWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.tick(ctx); err != nil {
				log.Printf("embedding worker: %v", err)
			}
		}
	}
}

func (w *EmbeddingWorker) tick(ctx context.Context) error {
	pending, err := w.messages.FetchPendingForEmbedding(ctx, w.batchSize)
	if err != nil {
		return err
	}

	for _, msg := range pending {
		vec, err := w.ollama.Embed(ctx, msg.Text)
		if err != nil {
			log.Printf("embedding worker: embed message %d: %v", msg.ID, err)
			continue // leave pending; will be retried on the next tick
		}
		literal := repository.FormatVector(vec)
		if err := w.embeddings.Insert(ctx, msg.ID, literal, w.modelName); err != nil {
			log.Printf("embedding worker: store embedding for message %d: %v", msg.ID, err)
		}
	}
	return nil
}
