package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"topicspulse/internal/models"
	"topicspulse/internal/repository"
)

type IngestService struct {
	messages *repository.MessagesRepo
}

func NewIngestService(messages *repository.MessagesRepo) *IngestService {
	return &IngestService{messages: messages}
}

// ValidationError signals a bad request (as opposed to an internal error).
type ValidationError struct {
	Msg string
}

func (e *ValidationError) Error() string { return e.Msg }

// Ingest validates and stores a single message. Embedding computation is
// intentionally not triggered here — the background worker picks it up.
func (s *IngestService) Ingest(ctx context.Context, req models.IngestMessageRequest) (int64, error) {
	login := strings.TrimSpace(req.Login)
	if login == "" {
		return 0, &ValidationError{"login is required"}
	}
	text := strings.TrimSpace(req.Text)
	if text == "" {
		return 0, &ValidationError{"text is required"}
	}
	if !req.Source.Valid() {
		return 0, &ValidationError{fmt.Sprintf("source must be one of game, forum, telegram (got %q)", req.Source)}
	}
	if req.CreatedAt == "" {
		return 0, &ValidationError{"created_at is required"}
	}
	createdAt, err := time.Parse(time.RFC3339, req.CreatedAt)
	if err != nil {
		return 0, &ValidationError{fmt.Sprintf("created_at must be RFC3339 (got %q)", req.CreatedAt)}
	}

	id, err := s.messages.Insert(ctx, login, req.UserID, text, createdAt, req.Source)
	if err != nil {
		return 0, fmt.Errorf("ingest: %w", err)
	}
	return id, nil
}
