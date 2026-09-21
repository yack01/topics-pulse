package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"topicspulse/internal/models"
	"topicspulse/internal/repository"
)

// llmTopicsPayload is the JSON shape every topic-discovery/merge/user-topics
// prompt asks the model to return: {"topics": [...]}.
type llmTopicsPayload struct {
	Topics []llmTopic `json:"topics"`
}

type llmTopic struct {
	Name                     string  `json:"name"`
	Summary                  string  `json:"summary"`
	RepresentativeMessageIDs []int64 `json:"representative_message_ids"`
}

func formatMessagesForPrompt(msgs []repository.PeriodMessage) string {
	var b strings.Builder
	for _, m := range msgs {
		fmt.Fprintf(&b, "[id=%d] %s\n", m.ID, singleLine(m.Text))
	}
	return b.String()
}

func singleLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// buildDiscoveryPrompt asks the LLM to find candidate topics in one batch of
// (at most AnalysisBatchSize) messages. Message ids are included so later
// steps can look up embeddings / display the original text without relying
// on the model to reproduce text verbatim.
func buildDiscoveryPrompt(batch []repository.PeriodMessage, maxTopics int) string {
	return fmt.Sprintf(`Ты аналитик службы поддержки. Ниже список сообщений пользователей (RU/UA/EN), каждое с уникальным id.
Найди не более %d основных тем/проблем, которые поднимаются в этих сообщениях. Похожие по смыслу сообщения (даже если разными словами) объединяй в одну тему.

Сообщения:
%s

Ответь СТРОГО в формате JSON, без пояснений:
{"topics": [{"name": "короткое название темы", "summary": "1-2 предложения описания", "representative_message_ids": [id1, id2, id3]}]}

representative_message_ids — это id из списка выше, максимум 5 штук на тему, только реально относящиеся к этой теме.
Если явных общих тем нет, верни {"topics": []}.`, maxTopics, formatMessagesForPrompt(batch))
}

// buildMergePrompt asks the LLM to deduplicate/merge candidate topics
// gathered across batches into a final list of at most maxTopics.
func buildMergePrompt(candidates []models.CandidateTopic, maxTopics int) string {
	var b strings.Builder
	for i, c := range candidates {
		fmt.Fprintf(&b, "%d) %s — %s (ids: %v)\n", i+1, c.Name, c.Summary, c.RepresentativeIDs)
	}

	return fmt.Sprintf(`Ниже список тем-кандидатов, найденных в разных пакетах сообщений одной системы поддержки. Часть тем дублирует друг друга разными словами — объедини их.

Кандидаты:
%s

Верни не более %d итоговых, взаимно различных тем в формате JSON:
{"topics": [{"name": "...", "summary": "...", "representative_message_ids": [id1, id2, ...]}]}

Для representative_message_ids используй ТОЛЬКО id, которые встречались у объединяемых кандидатов (объедини их id, максимум 5 на итоговую тему). Не придумывай новые id.`, b.String(), maxTopics)
}

// buildUserTopicsPrompt asks the LLM to directly produce the final topic
// list (with message_count) for a single user's messages in one call — no
// separate vector-assignment pass, since per-user volumes are small enough
// for the model to see everything at once.
func buildUserTopicsPrompt(msgs []repository.PeriodMessage, maxTopics int) string {
	return fmt.Sprintf(`Ты аналитик службы поддержки. Ниже все сообщения одного пользователя за период (RU/UA/EN), каждое с уникальным id.
Определи не более %d основных тем/вопросов, которые поднимал этот пользователь. Объединяй похожие по смыслу сообщения в одну тему.

Сообщения:
%s

Ответь СТРОГО в формате JSON, без пояснений:
{"topics": [{"name": "короткое название темы", "summary": "1-2 предложения описания", "representative_message_ids": [id1, id2, id3]}]}

representative_message_ids — id из списка выше, относящиеся к этой теме (можно все, если их немного).
Если сообщений слишком мало или общих тем нет, верни {"topics": []}.`, maxTopics, formatMessagesForPrompt(msgs))
}

// parseLLMTopics extracts the {"topics": [...]} payload from a raw LLM
// response, tolerating minor formatting noise around the JSON object.
func parseLLMTopics(raw string) ([]llmTopic, error) {
	raw = strings.TrimSpace(raw)
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start == -1 || end == -1 || end < start {
		return nil, fmt.Errorf("no JSON object found in LLM response")
	}
	raw = raw[start : end+1]

	var payload llmTopicsPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil, fmt.Errorf("unmarshal LLM topics: %w", err)
	}
	return payload.Topics, nil
}
