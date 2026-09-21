package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"topicspulse/internal/models"
)

type TopicsRepo struct {
	pool *pgxpool.Pool
}

func NewTopicsRepo(pool *pgxpool.Pool) *TopicsRepo {
	return &TopicsRepo{pool: pool}
}

// StartRun records the beginning of a nightly analysis run for a period.
func (r *TopicsRepo) StartRun(ctx context.Context, periodKey string, from, to time.Time) (int64, error) {
	const q = `
		INSERT INTO topic_analysis_runs (period_key, period_from, period_to, status)
		VALUES ($1, $2, $3, 'running')
		RETURNING id`

	var id int64
	if err := r.pool.QueryRow(ctx, q, periodKey, from, to).Scan(&id); err != nil {
		return 0, fmt.Errorf("start analysis run: %w", err)
	}
	return id, nil
}

// FinishRun marks a run as completed or failed.
func (r *TopicsRepo) FinishRun(ctx context.Context, runID int64, status string, errMsg string) error {
	const q = `
		UPDATE topic_analysis_runs
		SET status = $2, finished_at = now(), error = NULLIF($3, '')
		WHERE id = $1`

	if _, err := r.pool.Exec(ctx, q, runID, status, errMsg); err != nil {
		return fmt.Errorf("finish analysis run %d: %w", runID, err)
	}
	return nil
}

// InsertTopics writes the final ranked topic list for a run.
func (r *TopicsRepo) InsertTopics(ctx context.Context, runID int64, topics []models.Topic) error {
	const q = `
		INSERT INTO topics (run_id, rank, name, summary, message_count, unique_users, sources, representative_messages, topic_score)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	batch := &pgx.Batch{}
	for _, t := range topics {
		batch.Queue(q, runID, t.Rank, t.Name, t.Summary, t.MessageCount, t.UniqueUsers, t.Sources, t.RepresentativeMessages, t.TopicScore)
	}

	results := r.pool.SendBatch(ctx, batch)
	defer results.Close()

	for range topics {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("insert topic batch item: %w", err)
		}
	}
	return nil
}

// LatestCompletedRun finds the most recently finished successful run for a
// period key, e.g. "7d". GET /topics only ever reads from here — it never
// triggers computation itself.
func (r *TopicsRepo) LatestCompletedRun(ctx context.Context, periodKey string) (runID int64, from, to time.Time, found bool, err error) {
	const q = `
		SELECT id, period_from, period_to
		FROM topic_analysis_runs
		WHERE period_key = $1 AND status = 'completed'
		ORDER BY finished_at DESC
		LIMIT 1`

	err = r.pool.QueryRow(ctx, q, periodKey).Scan(&runID, &from, &to)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, time.Time{}, time.Time{}, false, nil
		}
		return 0, time.Time{}, time.Time{}, false, fmt.Errorf("latest completed run: %w", err)
	}
	return runID, from, to, true, nil
}

// TopicsForRun returns the ranked topics for a run, capped at limit.
func (r *TopicsRepo) TopicsForRun(ctx context.Context, runID int64, limit int) ([]models.Topic, error) {
	const q = `
		SELECT rank, name, summary, message_count, unique_users, sources, representative_messages, topic_score
		FROM topics
		WHERE run_id = $1
		ORDER BY rank
		LIMIT $2`

	rows, err := r.pool.Query(ctx, q, runID, limit)
	if err != nil {
		return nil, fmt.Errorf("topics for run %d: %w", runID, err)
	}
	defer rows.Close()

	var out []models.Topic
	for rows.Next() {
		var t models.Topic
		if err := rows.Scan(&t.Rank, &t.Name, &t.Summary, &t.MessageCount, &t.UniqueUsers, &t.Sources, &t.RepresentativeMessages, &t.TopicScore); err != nil {
			return nil, fmt.Errorf("scan topic: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
