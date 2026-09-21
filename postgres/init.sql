-- TopicsPulse: initial schema
-- Applied automatically by the postgres image on first container start
-- (mounted into /docker-entrypoint-initdb.d/).

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ---------------------------------------------------------------------
-- messages: one row per ingested user message
-- ---------------------------------------------------------------------
CREATE TABLE messages (
    id             BIGSERIAL PRIMARY KEY,
    login          TEXT NOT NULL,
    user_id        BIGINT,
    text           TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    source         TEXT NOT NULL CHECK (source IN ('game', 'forum', 'telegram')),
    inserted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    search_vector  tsvector GENERATED ALWAYS AS (to_tsvector('russian', text)) STORED
);

CREATE INDEX idx_messages_login_created_at ON messages (login, created_at DESC);
CREATE INDEX idx_messages_created_at ON messages (created_at);
CREATE INDEX idx_messages_source ON messages (source);
CREATE INDEX idx_messages_search_vector ON messages USING GIN (search_vector);
CREATE INDEX idx_messages_text_trgm ON messages USING GIN (text gin_trgm_ops);

-- ---------------------------------------------------------------------
-- message_embeddings: filled asynchronously by the embedding worker.
-- Absence of a row for a given message_id == "embedding pending".
-- ---------------------------------------------------------------------
CREATE TABLE message_embeddings (
    message_id  BIGINT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    embedding   vector(1024) NOT NULL,
    model       TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_message_embeddings_hnsw
    ON message_embeddings USING hnsw (embedding vector_cosine_ops);

-- ---------------------------------------------------------------------
-- topic_analysis_runs / topics: results of the nightly batch analysis,
-- read (not computed) by GET /topics.
-- ---------------------------------------------------------------------
CREATE TABLE topic_analysis_runs (
    id           BIGSERIAL PRIMARY KEY,
    period_key   TEXT NOT NULL,              -- '24h' | '7d' | '30d'
    period_from  TIMESTAMPTZ NOT NULL,
    period_to    TIMESTAMPTZ NOT NULL,
    status       TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    error        TEXT
);

CREATE INDEX idx_topic_analysis_runs_period_lookup
    ON topic_analysis_runs (period_key, status, finished_at DESC);

CREATE TABLE topics (
    id                       BIGSERIAL PRIMARY KEY,
    run_id                   BIGINT NOT NULL REFERENCES topic_analysis_runs(id) ON DELETE CASCADE,
    rank                     INT NOT NULL,
    name                     TEXT NOT NULL,
    summary                  TEXT NOT NULL,
    message_count            INT NOT NULL,
    unique_users             INT NOT NULL,
    sources                  TEXT[] NOT NULL DEFAULT '{}',
    representative_messages  TEXT[] NOT NULL DEFAULT '{}',
    topic_score              DOUBLE PRECISION NOT NULL
);

CREATE INDEX idx_topics_run_id_rank ON topics (run_id, rank);
