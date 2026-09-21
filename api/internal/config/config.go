package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// HTTP
	HTTPAddr string

	// Postgres
	PostgresDSN string

	// Ollama
	OllamaBaseURL        string
	OllamaEmbedModel     string
	OllamaLLMModel       string
	OllamaKeepAlive      string
	OllamaRequestTimeout time.Duration

	// Embedding worker
	EmbeddingPollInterval time.Duration
	EmbeddingBatchSize    int

	// Nightly analysis
	AnalysisCronExpr      string
	AnalysisPeriods       []string // "24h", "7d", "30d"
	AnalysisSampleSize    int
	AnalysisBatchSize     int
	AnalysisMaxTopics     int
	AnalysisMinSimilarity float64

	// Per-user on-demand analysis
	UserTopicsMaxMessages int
	UserTopicsMaxTopics   int

	// Full-text search
	SearchDefaultLimit int
	SearchMaxLimit     int
}

func Load() (*Config, error) {
	cfg := &Config{
		HTTPAddr:              getEnv("HTTP_ADDR", ":8080"),
		PostgresDSN:           getEnv("POSTGRES_DSN", ""),
		OllamaBaseURL:         getEnv("OLLAMA_BASE_URL", "http://ollama:11434"),
		OllamaEmbedModel:      getEnv("OLLAMA_EMBEDDING_MODEL", "bge-m3"),
		OllamaLLMModel:        getEnv("OLLAMA_LLM_MODEL", "qwen2.5:3b"),
		OllamaKeepAlive:       getEnv("OLLAMA_KEEP_ALIVE", "5m"),
		OllamaRequestTimeout:  getDurationEnv("OLLAMA_REQUEST_TIMEOUT", 120*time.Second),
		EmbeddingPollInterval: getDurationEnv("EMBEDDING_POLL_INTERVAL", 5*time.Second),
		EmbeddingBatchSize:    getIntEnv("EMBEDDING_BATCH_SIZE", 20),
		AnalysisCronExpr:      getEnv("ANALYSIS_CRON", "0 3 * * *"),
		AnalysisPeriods:       splitCSV(getEnv("ANALYSIS_PERIODS", "24h,7d,30d")),
		AnalysisSampleSize:    getIntEnv("ANALYSIS_SAMPLE_SIZE", 1200),
		AnalysisBatchSize:     getIntEnv("ANALYSIS_BATCH_SIZE", 200),
		AnalysisMaxTopics:     getIntEnv("ANALYSIS_MAX_TOPICS", 20),
		AnalysisMinSimilarity: getFloatEnv("ANALYSIS_MIN_SIMILARITY", 0.45),
		UserTopicsMaxMessages: getIntEnv("USER_TOPICS_MAX_MESSAGES", 500),
		UserTopicsMaxTopics:   getIntEnv("USER_TOPICS_MAX_TOPICS", 10),
		SearchDefaultLimit:    getIntEnv("SEARCH_DEFAULT_LIMIT", 20),
		SearchMaxLimit:        getIntEnv("SEARCH_MAX_LIMIT", 100),
	}

	if cfg.PostgresDSN == "" {
		return nil, fmt.Errorf("POSTGRES_DSN is required")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getFloatEnv(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// PeriodDuration converts a fixed period key ("24h", "7d", "30d") into a
// time.Duration. Only 'h' and 'd' suffixes are supported, which is all the
// config is expected to contain.
func PeriodDuration(periodKey string) (time.Duration, error) {
	if strings.HasSuffix(periodKey, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(periodKey, "d"))
		if err != nil {
			return 0, fmt.Errorf("invalid period %q: %w", periodKey, err)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	if strings.HasSuffix(periodKey, "h") {
		n, err := strconv.Atoi(strings.TrimSuffix(periodKey, "h"))
		if err != nil {
			return 0, fmt.Errorf("invalid period %q: %w", periodKey, err)
		}
		return time.Duration(n) * time.Hour, nil
	}
	return 0, fmt.Errorf("unsupported period format %q (expected e.g. 24h, 7d)", periodKey)
}
