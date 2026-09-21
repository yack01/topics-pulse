package handlers

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"topicspulse/internal/config"
	"topicspulse/internal/models"
	"topicspulse/internal/service"
)

type Handlers struct {
	cfg        *config.Config
	ingest     *service.IngestService
	analysis   *service.AnalysisService
	userTopics *service.UserTopicsService
	search     *service.SearchService
}

func New(cfg *config.Config, ingest *service.IngestService, analysis *service.AnalysisService, userTopics *service.UserTopicsService, search *service.SearchService) *Handlers {
	return &Handlers{cfg: cfg, ingest: ingest, analysis: analysis, userTopics: userTopics, search: search}
}

func (h *Handlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /messages", h.postMessage)
	mux.HandleFunc("GET /topics", h.getTopics)
	mux.HandleFunc("GET /users/{login}/topics", h.getUserTopics)
	mux.HandleFunc("GET /messages/search", h.searchMessages)
	mux.HandleFunc("POST /internal/analyze", h.triggerAnalysis)
}

func (h *Handlers) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handlers) postMessage(w http.ResponseWriter, r *http.Request) {
	var req models.IngestMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	id, err := h.ingest.Ingest(r.Context(), req)
	if err != nil {
		var verr *service.ValidationError
		if errors.As(err, &verr) {
			writeError(w, http.StatusBadRequest, verr.Error())
			return
		}
		log.Printf("postMessage: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

// getTopics handles GET /topics?period=7d&limit=10. It only ever reads a
// precomputed nightly result — see AnalysisService.GetPrecomputed.
func (h *Handlers) getTopics(w http.ResponseWriter, r *http.Request) {
	period := r.URL.Query().Get("period")
	if period == "" {
		period = "7d"
	}
	if !isSupportedPeriod(h.cfg.AnalysisPeriods, period) {
		writeError(w, http.StatusBadRequest, "period must be one of: "+joinCSV(h.cfg.AnalysisPeriods))
		return
	}

	limit := h.cfg.AnalysisMaxTopics
	if l, err := parseIntQuery(r, "limit"); err == nil && l > 0 {
		limit = l
	}

	resp, found, err := h.analysis.GetPrecomputed(r.Context(), period, limit)
	if err != nil {
		log.Printf("getTopics: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "no analysis result yet for period "+period+"; the nightly job may not have run yet")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// getUserTopics handles GET /users/{login}/topics?from=&to=&limit=,
// computed on demand.
func (h *Handlers) getUserTopics(w http.ResponseWriter, r *http.Request) {
	login := r.PathValue("login")
	if login == "" {
		writeError(w, http.StatusBadRequest, "login is required")
		return
	}

	from, to, err := parseFromTo(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	limit := 0
	if l, err := parseIntQuery(r, "limit"); err == nil {
		limit = l
	}

	resp, err := h.userTopics.Analyze(r.Context(), login, from, to, limit)
	if err != nil {
		var verr *service.ValidationError
		if errors.As(err, &verr) {
			writeError(w, http.StatusBadRequest, verr.Error())
			return
		}
		log.Printf("getUserTopics: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handlers) searchMessages(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	params := service.SearchParams{
		Query:  q.Get("q"),
		Login:  q.Get("login"),
		Source: q.Get("source"),
	}
	if l, err := parseIntQuery(r, "limit"); err == nil {
		params.Limit = l
	}
	if o, err := parseIntQuery(r, "offset"); err == nil {
		params.Offset = o
	}
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "from must be RFC3339")
			return
		}
		params.From = &t
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "to must be RFC3339")
			return
		}
		params.To = &t
	}

	resp, err := h.search.Search(r.Context(), params)
	if err != nil {
		var verr *service.ValidationError
		if errors.As(err, &verr) {
			writeError(w, http.StatusBadRequest, verr.Error())
			return
		}
		log.Printf("searchMessages: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// triggerAnalysis lets an operator run the nightly job on demand (e.g. for
// testing) instead of waiting for the cron schedule. Runs synchronously and
// can take a while — intended for manual/internal use only.
func (h *Handlers) triggerAnalysis(w http.ResponseWriter, r *http.Request) {
	h.analysis.RunNightly(r.Context())
	writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func parseFromTo(r *http.Request) (time.Time, time.Time, error) {
	q := r.URL.Query()
	fromStr, toStr := q.Get("from"), q.Get("to")
	if fromStr == "" || toStr == "" {
		return time.Time{}, time.Time{}, errFromToRequired
	}
	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		return time.Time{}, time.Time{}, errFromToFormat
	}
	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		return time.Time{}, time.Time{}, errFromToFormat
	}
	return from, to, nil
}

var (
	errFromToRequired = errors.New("from and to are required (RFC3339)")
	errFromToFormat   = errors.New("from/to must be RFC3339")
)

func parseIntQuery(r *http.Request, key string) (int, error) {
	v := r.URL.Query().Get(key)
	if v == "" {
		return 0, errors.New("empty")
	}
	return strconv.Atoi(v)
}

func isSupportedPeriod(supported []string, p string) bool {
	for _, s := range supported {
		if s == p {
			return true
		}
	}
	return false
}

func joinCSV(items []string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ", "
		}
		out += s
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("writeJSON: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
