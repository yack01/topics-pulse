package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type EmbeddingsRepo struct {
	pool *pgxpool.Pool
}

func NewEmbeddingsRepo(pool *pgxpool.Pool) *EmbeddingsRepo {
	return &EmbeddingsRepo{pool: pool}
}

// Insert stores the embedding for a message. vector must already be
// formatted as a pgvector text literal (see FormatVector).
func (r *EmbeddingsRepo) Insert(ctx context.Context, messageID int64, vectorLiteral string, model string) error {
	const q = `
		INSERT INTO message_embeddings (message_id, embedding, model)
		VALUES ($1, $2::vector, $3)
		ON CONFLICT (message_id) DO NOTHING`

	if _, err := r.pool.Exec(ctx, q, messageID, vectorLiteral, model); err != nil {
		return fmt.Errorf("insert embedding for message %d: %w", messageID, err)
	}
	return nil
}

// FetchByMessageIDs returns the stored embeddings (as pgvector text
// literals) for a set of message ids, used to build a topic centroid.
func (r *EmbeddingsRepo) FetchByMessageIDs(ctx context.Context, ids []int64) (map[int64]string, error) {
	if len(ids) == 0 {
		return map[int64]string{}, nil
	}

	const q = `SELECT message_id, embedding::text FROM message_embeddings WHERE message_id = ANY($1)`
	rows, err := r.pool.Query(ctx, q, ids)
	if err != nil {
		return nil, fmt.Errorf("fetch embeddings by ids: %w", err)
	}
	defer rows.Close()

	out := make(map[int64]string, len(ids))
	for rows.Next() {
		var id int64
		var vecText string
		if err := rows.Scan(&id, &vecText); err != nil {
			return nil, fmt.Errorf("scan embedding: %w", err)
		}
		out[id] = vecText
	}
	return out, rows.Err()
}

// TopicAssignment is the aggregated outcome of assigning every message in a
// period to its nearest topic centroid (cosine similarity, computed in SQL).
type TopicAssignment struct {
	TopicIdx     int
	MessageCount int
	UniqueUsers  int
	Sources      []string
}

// AssignMessagesToTopics computes, for every message in [from, to) that has
// an embedding, the nearest topic centroid (by cosine similarity) and
// aggregates counts per topic. Messages whose best similarity is below
// minSimilarity are treated as not belonging to any discovered topic and
// excluded. This is the "vector assignment" step that replaces full
// clustering: it reuses the embeddings the background worker already
// computed, at the cost of one SQL query with N centroids.
func (r *EmbeddingsRepo) AssignMessagesToTopics(ctx context.Context, from, to time.Time, centroids []string, minSimilarity float64) ([]TopicAssignment, error) {
	if len(centroids) == 0 {
		return nil, nil
	}

	values := make([]string, len(centroids))
	args := []any{from, to}
	for i, c := range centroids {
		args = append(args, c)
		values[i] = fmt.Sprintf("(%d, $%d::vector)", i, len(args))
	}
	minSimIdx := len(args) + 1
	args = append(args, minSimilarity)

	q := fmt.Sprintf(`
		WITH candidates AS (
			SELECT m.id AS message_id, m.login, m.source, tc.topic_idx,
			       1 - (me.embedding <=> tc.centroid) AS similarity
			FROM messages m
			JOIN message_embeddings me ON me.message_id = m.id
			CROSS JOIN (VALUES %s) AS tc(topic_idx, centroid)
			WHERE m.created_at >= $1 AND m.created_at < $2
		),
		best AS (
			SELECT DISTINCT ON (message_id) message_id, login, source, topic_idx, similarity
			FROM candidates
			ORDER BY message_id, similarity DESC
		)
		SELECT topic_idx, count(*) AS message_count, count(DISTINCT login) AS unique_users,
		       array_agg(DISTINCT source) AS sources
		FROM best
		WHERE similarity >= $%d
		GROUP BY topic_idx`, strings.Join(values, ", "), minSimIdx)

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("assign messages to topics: %w", err)
	}
	defer rows.Close()

	var out []TopicAssignment
	for rows.Next() {
		var a TopicAssignment
		if err := rows.Scan(&a.TopicIdx, &a.MessageCount, &a.UniqueUsers, &a.Sources); err != nil {
			return nil, fmt.Errorf("scan topic assignment: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
