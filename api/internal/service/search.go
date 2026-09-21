package service

import (
	"context"
	"fmt"
	"time"

	"topicspulse/internal/config"
	"topicspulse/internal/models"
	"topicspulse/internal/repository"
)

type SearchService struct {
	cfg  *config.Config
	repo *repository.SearchRepo
}

func NewSearchService(cfg *config.Config, repo *repository.SearchRepo) *SearchService {
	return &SearchService{cfg: cfg, repo: repo}
}

type SearchParams struct {
	Query  string
	Login  string
	Source string
	From   *time.Time
	To     *time.Time
	Limit  int
	Offset int
}

func (s *SearchService) Search(ctx context.Context, p SearchParams) (*models.SearchResponse, error) {
	if p.Query == "" {
		return nil, &ValidationError{"q is required"}
	}
	if p.Source != "" && !models.Source(p.Source).Valid() {
		return nil, &ValidationError{fmt.Sprintf("source must be one of game, forum, telegram (got %q)", p.Source)}
	}

	limit := p.Limit
	if limit <= 0 {
		limit = s.cfg.SearchDefaultLimit
	}
	if limit > s.cfg.SearchMaxLimit {
		limit = s.cfg.SearchMaxLimit
	}

	results, err := s.repo.Search(ctx, repository.SearchFilter{
		Query:  p.Query,
		Login:  p.Login,
		Source: p.Source,
		From:   p.From,
		To:     p.To,
		Limit:  limit,
		Offset: p.Offset,
	})
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	return &models.SearchResponse{Query: p.Query, Total: len(results), Results: results}, nil
}
