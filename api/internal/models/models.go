package models

import "time"

// Source is one of the fixed ingestion channels.
type Source string

const (
	SourceGame     Source = "game"
	SourceForum    Source = "forum"
	SourceTelegram Source = "telegram"
)

func (s Source) Valid() bool {
	switch s {
	case SourceGame, SourceForum, SourceTelegram:
		return true
	default:
		return false
	}
}

// Message is a single ingested user message.
type Message struct {
	ID         int64     `json:"id"`
	Login      string    `json:"login"`
	UserID     *int64    `json:"user_id,omitempty"`
	Text       string    `json:"text"`
	CreatedAt  time.Time `json:"created_at"`
	Source     Source    `json:"source"`
	InsertedAt time.Time `json:"inserted_at"`
}

// IngestMessageRequest is the POST /messages payload.
type IngestMessageRequest struct {
	Login     string `json:"login"`
	UserID    *int64 `json:"user_id,omitempty"`
	Text      string `json:"text"`
	CreatedAt string `json:"created_at"`
	Source    Source `json:"source"`
}

// Topic is one ranked topic in an analysis result.
type Topic struct {
	Rank                   int      `json:"rank"`
	Name                   string   `json:"name"`
	Summary                string   `json:"summary"`
	MessageCount           int      `json:"message_count"`
	UniqueUsers            int      `json:"unique_users"`
	Sources                []string `json:"sources"`
	RepresentativeMessages []string `json:"representative_messages"`
	TopicScore             float64  `json:"topic_score"`
}

// TopicsResponse is the shared response shape for /topics and /users/{login}/topics.
type TopicsResponse struct {
	From   time.Time `json:"from"`
	To     time.Time `json:"to"`
	Topics []Topic   `json:"topics"`
}

// SearchResult is one row returned by GET /messages/search.
type SearchResult struct {
	ID        int64     `json:"id"`
	Login     string    `json:"login"`
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"created_at"`
	Source    Source    `json:"source"`
	Rank      float64   `json:"rank"`
}

// SearchResponse wraps SearchResult rows.
type SearchResponse struct {
	Query   string         `json:"query"`
	Total   int            `json:"total"`
	Results []SearchResult `json:"results"`
}

// candidateTopic is the intermediate shape produced by the per-batch LLM
// calls during nightly analysis, before merge + vector assignment.
type CandidateTopic struct {
	Name              string  `json:"name"`
	Summary           string  `json:"summary"`
	RepresentativeIDs []int64 `json:"representative_message_ids"`
}
