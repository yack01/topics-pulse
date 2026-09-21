package service

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"topicspulse/internal/config"
	"topicspulse/internal/models"
	"topicspulse/internal/ollama"
	"topicspulse/internal/repository"
)

const maxRepresentativeIDsPerTopic = 5

// AnalysisService implements the nightly, no-clustering topic pipeline for
// GET /topics:
//
//	sample messages -> LLM finds candidate topics per batch -> LLM merges
//	candidates -> centroid per final topic (avg of representative embeddings)
//	-> assign every message in the period to its nearest centroid (SQL,
//	cosine similarity) -> score & rank -> store.
//
// GET /topics itself never runs any of this; it only reads the latest
// completed run (see repository.TopicsRepo.LatestCompletedRun).
type AnalysisService struct {
	cfg        *config.Config
	messages   *repository.MessagesRepo
	embeddings *repository.EmbeddingsRepo
	topics     *repository.TopicsRepo
	llm        *ollama.Client
}

func NewAnalysisService(cfg *config.Config, messages *repository.MessagesRepo, embeddings *repository.EmbeddingsRepo, topics *repository.TopicsRepo, llm *ollama.Client) *AnalysisService {
	return &AnalysisService{cfg: cfg, messages: messages, embeddings: embeddings, topics: topics, llm: llm}
}

// RunNightly analyzes every configured period (e.g. 24h, 7d, 30d). A
// failure on one period is logged and does not prevent the others from
// running.
func (s *AnalysisService) RunNightly(ctx context.Context) {
	for _, periodKey := range s.cfg.AnalysisPeriods {
		if err := s.runPeriod(ctx, periodKey); err != nil {
			log.Printf("nightly analysis: period %s failed: %v", periodKey, err)
		}
	}
}

func (s *AnalysisService) runPeriod(ctx context.Context, periodKey string) error {
	duration, err := config.PeriodDuration(periodKey)
	if err != nil {
		return err
	}
	to := time.Now().UTC()
	from := to.Add(-duration)

	runID, err := s.topics.StartRun(ctx, periodKey, from, to)
	if err != nil {
		return err
	}

	if runErr := s.analyzePeriod(ctx, runID, from, to); runErr != nil {
		_ = s.topics.FinishRun(ctx, runID, "failed", runErr.Error())
		return fmt.Errorf("period %s: %w", periodKey, runErr)
	}

	return s.topics.FinishRun(ctx, runID, "completed", "")
}

func (s *AnalysisService) analyzePeriod(ctx context.Context, runID int64, from, to time.Time) error {
	sample, err := s.messages.SampleInPeriod(ctx, from, to, s.cfg.AnalysisSampleSize)
	if err != nil {
		return fmt.Errorf("sample messages: %w", err)
	}
	if len(sample) == 0 {
		return s.topics.InsertTopics(ctx, runID, nil)
	}

	candidates := s.discoverCandidates(ctx, sample)
	if len(candidates) == 0 {
		return s.topics.InsertTopics(ctx, runID, nil)
	}

	merged, err := s.mergeCandidates(ctx, candidates, sample)
	if err != nil {
		return fmt.Errorf("merge candidates: %w", err)
	}
	if len(merged) == 0 {
		return s.topics.InsertTopics(ctx, runID, nil)
	}

	finalTopics, err := s.scoreAndAssign(ctx, merged, from, to)
	if err != nil {
		return fmt.Errorf("assign & score topics: %w", err)
	}

	return s.topics.InsertTopics(ctx, runID, finalTopics)
}

// discoverCandidates runs one LLM call per batch of the sample.
func (s *AnalysisService) discoverCandidates(ctx context.Context, sample []repository.PeriodMessage) []models.CandidateTopic {
	validIDs := idSet(sample)
	var candidates []models.CandidateTopic

	for start := 0; start < len(sample); start += s.cfg.AnalysisBatchSize {
		end := start + s.cfg.AnalysisBatchSize
		if end > len(sample) {
			end = len(sample)
		}
		batch := sample[start:end]

		prompt := buildDiscoveryPrompt(batch, s.cfg.AnalysisMaxTopics)
		raw, err := s.llm.Generate(ctx, prompt, true)
		if err != nil {
			log.Printf("nightly analysis: discovery batch [%d:%d] LLM call failed: %v", start, end, err)
			continue
		}

		parsed, err := parseLLMTopics(raw)
		if err != nil {
			log.Printf("nightly analysis: discovery batch [%d:%d] unparseable response: %v", start, end, err)
			continue
		}

		for _, t := range parsed {
			ids := filterKnownIDs(t.RepresentativeMessageIDs, validIDs, maxRepresentativeIDsPerTopic)
			if len(ids) == 0 || t.Name == "" {
				continue
			}
			candidates = append(candidates, models.CandidateTopic{
				Name:              t.Name,
				Summary:           t.Summary,
				RepresentativeIDs: ids,
			})
		}
	}
	return candidates
}

// mergeCandidates asks the LLM to deduplicate candidates from all batches
// into at most AnalysisMaxTopics final topics.
func (s *AnalysisService) mergeCandidates(ctx context.Context, candidates []models.CandidateTopic, sample []repository.PeriodMessage) ([]models.CandidateTopic, error) {
	if len(candidates) <= s.cfg.AnalysisMaxTopics {
		// Nothing to merge; still dedup-safe to return as-is.
		return candidates, nil
	}

	validIDs := idSet(sample)
	prompt := buildMergePrompt(candidates, s.cfg.AnalysisMaxTopics)
	raw, err := s.llm.Generate(ctx, prompt, true)
	if err != nil {
		return nil, fmt.Errorf("merge LLM call: %w", err)
	}

	parsed, err := parseLLMTopics(raw)
	if err != nil {
		return nil, fmt.Errorf("merge response: %w", err)
	}

	var merged []models.CandidateTopic
	for _, t := range parsed {
		ids := filterKnownIDs(t.RepresentativeMessageIDs, validIDs, maxRepresentativeIDsPerTopic)
		if len(ids) == 0 || t.Name == "" {
			continue
		}
		merged = append(merged, models.CandidateTopic{Name: t.Name, Summary: t.Summary, RepresentativeIDs: ids})
	}
	return merged, nil
}

// scoreAndAssign builds a centroid per candidate topic, assigns every
// message in [from, to) to its nearest centroid via SQL, computes
// topic_score = norm(volume) + norm(unique_users), and returns the final
// ranked list.
func (s *AnalysisService) scoreAndAssign(ctx context.Context, candidates []models.CandidateTopic, from, to time.Time) ([]models.Topic, error) {
	centroids := make([]string, 0, len(candidates))
	keptCandidates := make([]models.CandidateTopic, 0, len(candidates))

	for _, c := range candidates {
		vecMap, err := s.embeddings.FetchByMessageIDs(ctx, c.RepresentativeIDs)
		if err != nil {
			return nil, err
		}
		var vectors [][]float64
		for _, id := range c.RepresentativeIDs {
			if lit, ok := vecMap[id]; ok {
				v, err := repository.ParseVector(lit)
				if err != nil {
					continue
				}
				vectors = append(vectors, v)
			}
		}
		if len(vectors) == 0 {
			continue // representative messages not embedded yet; skip this topic this run
		}
		centroid, err := repository.AverageVectors(vectors)
		if err != nil {
			continue
		}
		centroids = append(centroids, repository.FormatVector64(centroid))
		keptCandidates = append(keptCandidates, c)
	}

	if len(centroids) == 0 {
		return nil, nil
	}

	assignments, err := s.embeddings.AssignMessagesToTopics(ctx, from, to, centroids, s.cfg.AnalysisMinSimilarity)
	if err != nil {
		return nil, err
	}

	byIdx := make(map[int]repository.TopicAssignment, len(assignments))
	for _, a := range assignments {
		byIdx[a.TopicIdx] = a
	}

	type scored struct {
		candidate models.CandidateTopic
		volume    int
		users     int
		sources   []string
	}
	var withStats []scored
	minVol, maxVol := 0, 0
	minUsers, maxUsers := 0, 0
	first := true
	for i, c := range keptCandidates {
		a, ok := byIdx[i]
		if !ok || a.MessageCount == 0 {
			continue // no message in the period actually matched this topic closely enough
		}
		withStats = append(withStats, scored{candidate: c, volume: a.MessageCount, users: a.UniqueUsers, sources: a.Sources})
		if first {
			minVol, maxVol = a.MessageCount, a.MessageCount
			minUsers, maxUsers = a.UniqueUsers, a.UniqueUsers
			first = false
			continue
		}
		if a.MessageCount < minVol {
			minVol = a.MessageCount
		}
		if a.MessageCount > maxVol {
			maxVol = a.MessageCount
		}
		if a.UniqueUsers < minUsers {
			minUsers = a.UniqueUsers
		}
		if a.UniqueUsers > maxUsers {
			maxUsers = a.UniqueUsers
		}
	}

	var allRepIDs []int64
	for _, ws := range withStats {
		allRepIDs = append(allRepIDs, ws.candidate.RepresentativeIDs...)
	}
	repTexts, err := s.fetchRepresentativeTexts(ctx, allRepIDs)
	if err != nil {
		return nil, err
	}

	result := make([]models.Topic, 0, len(withStats))
	for _, ws := range withStats {
		var texts []string
		for _, id := range ws.candidate.RepresentativeIDs {
			if t, ok := repTexts[id]; ok {
				texts = append(texts, t)
			}
		}
		score := normalize(float64(ws.volume), float64(minVol), float64(maxVol)) +
			normalize(float64(ws.users), float64(minUsers), float64(maxUsers))

		result = append(result, models.Topic{
			Name:                   ws.candidate.Name,
			Summary:                ws.candidate.Summary,
			MessageCount:           ws.volume,
			UniqueUsers:            ws.users,
			Sources:                ws.sources,
			RepresentativeMessages: texts,
			TopicScore:             score,
		})
	}

	sortTopicsByScoreDesc(result)
	if len(result) > s.cfg.AnalysisMaxTopics {
		result = result[:s.cfg.AnalysisMaxTopics]
	}
	for i := range result {
		result[i].Rank = i + 1
	}
	return result, nil
}

func (s *AnalysisService) fetchRepresentativeTexts(ctx context.Context, ids []int64) (map[int64]string, error) {
	return s.messages.TextsByIDs(ctx, ids)
}

// GetPrecomputed reads the latest completed nightly run for a period key.
// This is the ONLY code path behind GET /topics — no computation happens
// on request.
func (s *AnalysisService) GetPrecomputed(ctx context.Context, periodKey string, limit int) (*models.TopicsResponse, bool, error) {
	runID, from, to, found, err := s.topics.LatestCompletedRun(ctx, periodKey)
	if err != nil || !found {
		return nil, found, err
	}

	topics, err := s.topics.TopicsForRun(ctx, runID, limit)
	if err != nil {
		return nil, false, err
	}

	return &models.TopicsResponse{From: from, To: to, Topics: topics}, true, nil
}

func idSet(msgs []repository.PeriodMessage) map[int64]bool {
	set := make(map[int64]bool, len(msgs))
	for _, m := range msgs {
		set[m.ID] = true
	}
	return set
}

func filterKnownIDs(ids []int64, known map[int64]bool, max int) []int64 {
	var out []int64
	for _, id := range ids {
		if known[id] {
			out = append(out, id)
			if len(out) >= max {
				break
			}
		}
	}
	return out
}

func normalize(x, min, max float64) float64 {
	if max == min {
		return 0
	}
	return (x - min) / (max - min)
}

func sortTopicsByScoreDesc(topics []models.Topic) {
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].TopicScore > topics[j].TopicScore
	})
}
