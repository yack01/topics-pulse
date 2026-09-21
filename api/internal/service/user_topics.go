package service

import (
	"context"
	"fmt"
	"time"

	"topicspulse/internal/config"
	"topicspulse/internal/models"
	"topicspulse/internal/ollama"
	"topicspulse/internal/repository"
)

// UserTopicsService answers GET /users/{login}/topics synchronously: a
// single user's messages for a period are small enough that the LLM sees
// them all in one call and can report message_count itself directly, no
// embeddings or vector-assignment pass needed.
type UserTopicsService struct {
	cfg      *config.Config
	messages *repository.MessagesRepo
	llm      *ollama.Client
}

func NewUserTopicsService(cfg *config.Config, messages *repository.MessagesRepo, llm *ollama.Client) *UserTopicsService {
	return &UserTopicsService{cfg: cfg, messages: messages, llm: llm}
}

func (s *UserTopicsService) Analyze(ctx context.Context, login string, from, to time.Time, limit int) (*models.TopicsResponse, error) {
	msgs, err := s.messages.ByLoginInPeriod(ctx, login, from, to, s.cfg.UserTopicsMaxMessages)
	if err != nil {
		return nil, fmt.Errorf("fetch user messages: %w", err)
	}
	if len(msgs) == 0 {
		return &models.TopicsResponse{From: from, To: to, Topics: nil}, nil
	}

	if limit <= 0 || limit > s.cfg.UserTopicsMaxTopics {
		limit = s.cfg.UserTopicsMaxTopics
	}

	prompt := buildUserTopicsPrompt(msgs, limit)
	raw, err := s.llm.Generate(ctx, prompt, true)
	if err != nil {
		return nil, fmt.Errorf("llm call: %w", err)
	}

	parsed, err := parseLLMTopics(raw)
	if err != nil {
		return nil, fmt.Errorf("parse llm response: %w", err)
	}

	byID := make(map[int64]repository.PeriodMessage, len(msgs))
	validIDs := make(map[int64]bool, len(msgs))
	sourcesByID := make(map[int64]string, len(msgs))
	for _, m := range msgs {
		byID[m.ID] = m
		validIDs[m.ID] = true
		sourcesByID[m.ID] = m.Source
	}

	topics := make([]models.Topic, 0, len(parsed))
	for _, t := range parsed {
		ids := filterKnownIDs(t.RepresentativeMessageIDs, validIDs, s.cfg.UserTopicsMaxMessages)
		if len(ids) == 0 || t.Name == "" {
			continue
		}

		sourceSet := map[string]bool{}
		texts := make([]string, 0, len(ids))
		for _, id := range ids {
			sourceSet[sourcesByID[id]] = true
			texts = append(texts, byID[id].Text)
		}
		var sources []string
		for src := range sourceSet {
			sources = append(sources, src)
		}

		displayTexts := texts
		if len(displayTexts) > maxRepresentativeIDsPerTopic {
			displayTexts = displayTexts[:maxRepresentativeIDsPerTopic]
		}

		topics = append(topics, models.Topic{
			Name:                   t.Name,
			Summary:                t.Summary,
			MessageCount:           len(ids),
			UniqueUsers:            1,
			Sources:                sources,
			RepresentativeMessages: displayTexts,
			TopicScore:             float64(len(ids)),
		})
	}

	sortTopicsByScoreDesc(topics)
	if len(topics) > limit {
		topics = topics[:limit]
	}
	for i := range topics {
		topics[i].Rank = i + 1
	}

	return &models.TopicsResponse{From: from, To: to, Topics: topics}, nil
}
