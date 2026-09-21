package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"topicspulse/internal/models"
)

type MessagesRepo struct {
	pool *pgxpool.Pool
}

func NewMessagesRepo(pool *pgxpool.Pool) *MessagesRepo {
	return &MessagesRepo{pool: pool}
}

// Insert stores a new message and returns its generated id. No idempotency
// or dedup checks are performed by design (see brief section 17).
func (r *MessagesRepo) Insert(ctx context.Context, login string, userID *int64, text string, createdAt time.Time, source models.Source) (int64, error) {
	const q = `
		INSERT INTO messages (login, user_id, text, created_at, source)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`

	var id int64
	if err := r.pool.QueryRow(ctx, q, login, userID, text, createdAt, string(source)).Scan(&id); err != nil {
		return 0, fmt.Errorf("insert message: %w", err)
	}
	return id, nil
}

// PendingEmbedding is a message that has not yet been embedded.
type PendingEmbedding struct {
	ID   int64
	Text string
}

// FetchPendingForEmbedding returns up to `limit` messages that don't have a
// row in message_embeddings yet, oldest first.
func (r *MessagesRepo) FetchPendingForEmbedding(ctx context.Context, limit int) ([]PendingEmbedding, error) {
	const q = `
		SELECT m.id, m.text
		FROM messages m
		LEFT JOIN message_embeddings e ON e.message_id = m.id
		WHERE e.message_id IS NULL
		ORDER BY m.id
		LIMIT $1`

	rows, err := r.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch pending embeddings: %w", err)
	}
	defer rows.Close()

	var out []PendingEmbedding
	for rows.Next() {
		var p PendingEmbedding
		if err := rows.Scan(&p.ID, &p.Text); err != nil {
			return nil, fmt.Errorf("scan pending embedding: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// PeriodMessage is a lightweight projection used for LLM-based topic
// discovery (sampling) and per-user on-demand analysis.
type PeriodMessage struct {
	ID        int64
	Login     string
	Text      string
	Source    string
	CreatedAt time.Time
}

// CountInPeriod returns the total number of messages in [from, to).
func (r *MessagesRepo) CountInPeriod(ctx context.Context, from, to time.Time) (int, error) {
	const q = `SELECT count(*) FROM messages WHERE created_at >= $1 AND created_at < $2`
	var n int
	if err := r.pool.QueryRow(ctx, q, from, to).Scan(&n); err != nil {
		return 0, fmt.Errorf("count messages in period: %w", err)
	}
	return n, nil
}

// SampleInPeriod returns a random sample of up to `limit` messages within
// [from, to). Used as the input to LLM-based topic discovery so the model
// never has to read an entire period's worth of messages at once.
func (r *MessagesRepo) SampleInPeriod(ctx context.Context, from, to time.Time, limit int) ([]PeriodMessage, error) {
	const q = `
		SELECT id, login, text, source, created_at
		FROM messages
		WHERE created_at >= $1 AND created_at < $2
		ORDER BY random()
		LIMIT $3`

	rows, err := r.pool.Query(ctx, q, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("sample messages in period: %w", err)
	}
	defer rows.Close()
	return scanPeriodMessages(rows)
}

// ByLoginInPeriod returns up to `limit` messages for a single user within
// [from, to), most recent first. Used by the on-demand per-user endpoint.
func (r *MessagesRepo) ByLoginInPeriod(ctx context.Context, login string, from, to time.Time, limit int) ([]PeriodMessage, error) {
	const q = `
		SELECT id, login, text, source, created_at
		FROM messages
		WHERE login = $1 AND created_at >= $2 AND created_at < $3
		ORDER BY created_at DESC
		LIMIT $4`

	rows, err := r.pool.Query(ctx, q, login, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch messages by login: %w", err)
	}
	defer rows.Close()
	return scanPeriodMessages(rows)
}

// TextsByIDs returns message text keyed by id, used to render
// representative_messages for a topic after LLM discovery.
func (r *MessagesRepo) TextsByIDs(ctx context.Context, ids []int64) (map[int64]string, error) {
	if len(ids) == 0 {
		return map[int64]string{}, nil
	}

	const q = `SELECT id, text FROM messages WHERE id = ANY($1)`
	rows, err := r.pool.Query(ctx, q, ids)
	if err != nil {
		return nil, fmt.Errorf("texts by ids: %w", err)
	}
	defer rows.Close()

	out := make(map[int64]string, len(ids))
	for rows.Next() {
		var id int64
		var text string
		if err := rows.Scan(&id, &text); err != nil {
			return nil, fmt.Errorf("scan text: %w", err)
		}
		out[id] = text
	}
	return out, rows.Err()
}

func scanPeriodMessages(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) ([]PeriodMessage, error) {
	var out []PeriodMessage
	for rows.Next() {
		var m PeriodMessage
		if err := rows.Scan(&m.ID, &m.Login, &m.Text, &m.Source, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan period message: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
