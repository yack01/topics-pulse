package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"topicspulse/internal/models"
)

type SearchRepo struct {
	pool *pgxpool.Pool
}

func NewSearchRepo(pool *pgxpool.Pool) *SearchRepo {
	return &SearchRepo{pool: pool}
}

type SearchFilter struct {
	Query  string
	Login  string // optional
	Source string // optional
	From   *time.Time
	To     *time.Time
	Limit  int
	Offset int
}

// Search runs Postgres full-text search (tsvector/tsquery, "russian"
// config) over messages.text. If that yields no rows, it falls back to a
// pg_trgm similarity search, which tolerates typos and works reasonably for
// languages without a dedicated FTS dictionary (e.g. Ukrainian).
func (r *SearchRepo) Search(ctx context.Context, f SearchFilter) ([]models.SearchResult, error) {
	results, err := r.searchFTS(ctx, f)
	if err != nil {
		return nil, err
	}
	if len(results) > 0 {
		return results, nil
	}
	return r.searchTrigram(ctx, f)
}

func (r *SearchRepo) searchFTS(ctx context.Context, f SearchFilter) ([]models.SearchResult, error) {
	where, args := buildFilterClauses(f)
	where = append(where, fmt.Sprintf("search_vector @@ plainto_tsquery('russian', $%d)", len(args)+1))
	args = append(args, f.Query)

	rankIdx := len(args)
	args = append(args, f.Limit, f.Offset)

	q := fmt.Sprintf(`
		SELECT id, login, text, created_at, source,
		       ts_rank(search_vector, plainto_tsquery('russian', $%d)) AS rank
		FROM messages
		WHERE %s
		ORDER BY rank DESC, created_at DESC
		LIMIT $%d OFFSET $%d`,
		rankIdx, strings.Join(where, " AND "), len(args)-1, len(args))

	return runSearchQuery(ctx, r.pool, q, args)
}

func (r *SearchRepo) searchTrigram(ctx context.Context, f SearchFilter) ([]models.SearchResult, error) {
	where, args := buildFilterClauses(f)
	simIdx := len(args) + 1
	where = append(where, fmt.Sprintf("similarity(text, $%d) > 0.15", simIdx))
	args = append(args, f.Query)

	args = append(args, f.Limit, f.Offset)

	q := fmt.Sprintf(`
		SELECT id, login, text, created_at, source,
		       similarity(text, $%d) AS rank
		FROM messages
		WHERE %s
		ORDER BY rank DESC, created_at DESC
		LIMIT $%d OFFSET $%d`,
		simIdx, strings.Join(where, " AND "), len(args)-1, len(args))

	return runSearchQuery(ctx, r.pool, q, args)
}

func buildFilterClauses(f SearchFilter) ([]string, []any) {
	var where []string
	var args []any

	if f.Login != "" {
		args = append(args, f.Login)
		where = append(where, fmt.Sprintf("login = $%d", len(args)))
	}
	if f.Source != "" {
		args = append(args, f.Source)
		where = append(where, fmt.Sprintf("source = $%d", len(args)))
	}
	if f.From != nil {
		args = append(args, *f.From)
		where = append(where, fmt.Sprintf("created_at >= $%d", len(args)))
	}
	if f.To != nil {
		args = append(args, *f.To)
		where = append(where, fmt.Sprintf("created_at < $%d", len(args)))
	}
	if len(where) == 0 {
		where = append(where, "TRUE")
	}
	return where, args
}

func runSearchQuery(ctx context.Context, pool *pgxpool.Pool, q string, args []any) ([]models.SearchResult, error) {
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("search query: %w", err)
	}
	defer rows.Close()

	var out []models.SearchResult
	for rows.Next() {
		var res models.SearchResult
		var source string
		if err := rows.Scan(&res.ID, &res.Login, &res.Text, &res.CreatedAt, &source, &res.Rank); err != nil {
			return nil, fmt.Errorf("scan search result: %w", err)
		}
		res.Source = models.Source(source)
		out = append(out, res)
	}
	return out, rows.Err()
}
