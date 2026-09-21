// Package ollama is a minimal client for the subset of the Ollama HTTP API
// TopicsPulse needs: computing embeddings and generating text/JSON with a
// local LLM. Both models run inside the same `ollama` container; Ollama
// itself takes care of loading/unloading them from RAM (see OLLAMA_KEEP_ALIVE)
// which is what lets this stack fit into a tight RAM budget.
package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	baseURL    string
	embedModel string
	llmModel   string
	keepAlive  string
	httpClient *http.Client
}

func NewClient(baseURL, embedModel, llmModel, keepAlive string, timeout time.Duration) *Client {
	return &Client{
		baseURL:    baseURL,
		embedModel: embedModel,
		llmModel:   llmModel,
		keepAlive:  keepAlive,
		httpClient: &http.Client{Timeout: timeout},
	}
}

type embedRequest struct {
	Model     string `json:"model"`
	Prompt    string `json:"prompt"`
	KeepAlive string `json:"keep_alive,omitempty"`
}

type embedResponse struct {
	Embedding []float32 `json:"embedding"`
}

// Embed returns the embedding vector for a single piece of text using the
// configured embedding model (default: bge-m3, dimension 1024).
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	reqBody := embedRequest{
		Model:     c.embedModel,
		Prompt:    text,
		KeepAlive: c.keepAlive,
	}

	var out embedResponse
	if err := c.postJSON(ctx, "/api/embeddings", reqBody, &out); err != nil {
		return nil, err
	}
	if len(out.Embedding) == 0 {
		return nil, fmt.Errorf("ollama returned empty embedding")
	}
	return out.Embedding, nil
}

type generateRequest struct {
	Model     string `json:"model"`
	Prompt    string `json:"prompt"`
	Stream    bool   `json:"stream"`
	Format    string `json:"format,omitempty"`
	KeepAlive string `json:"keep_alive,omitempty"`
}

type generateResponse struct {
	Response string `json:"response"`
}

// Generate runs the configured LLM on a prompt and returns the raw text
// response. When jsonMode is true, Ollama is asked to constrain output to
// valid JSON (still returned as a string for the caller to unmarshal).
func (c *Client) Generate(ctx context.Context, prompt string, jsonMode bool) (string, error) {
	reqBody := generateRequest{
		Model:     c.llmModel,
		Prompt:    prompt,
		Stream:    false,
		KeepAlive: c.keepAlive,
	}
	if jsonMode {
		reqBody.Format = "json"
	}

	var out generateResponse
	if err := c.postJSON(ctx, "/api/generate", reqBody, &out); err != nil {
		return "", err
	}
	return out.Response, nil
}

func (c *Client) postJSON(ctx context.Context, path string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ollama request to %s failed: %w", path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama %s returned status %d: %s", path, resp.StatusCode, string(respBody))
	}

	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("decode response from %s: %w", path, err)
	}
	return nil
}
