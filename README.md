# TopicsPulse

Локальная система семантического анализа тем пользовательских сообщений — без внешних платных API, полностью в Docker. Принимает сообщения по одному, определяет актуальные темы за период (общие и по конкретному пользователю), умеет полнотекстовый поиск по всей истории.

## Что внутри

- **1 входной endpoint**: `POST /messages`.
- **3 выходных endpoint'а**: `GET /topics` (все темы), `GET /users/{login}/topics` (темы юзера), `GET /messages/search` (полнотекстовый поиск).
- **3 контейнера**: `postgres` (+ `pgvector`, `pg_trgm`), `ollama` (embeddings `bge-m3` + LLM для нейминга тем), `api` (Go — HTTP-сервер, фоновый embedding-worker и ночной cron анализа тем внутри одного бинарника).

### Архитектурные решения (кратко)

| Тема | Решение |
|---|---|
| Кластеризация | Не используется. LLM находит темы напрямую по выборке сообщений (map-reduce по батчам), затем каждое сообщение периода присваивается ближайшей теме по cosine similarity через уже посчитанные embeddings (`pgvector`) — без обучения кластерной модели. |
| `GET /topics` | Считается **ночным cron'ом** (по умолчанию 03:00) для фиксированных периодов `24h/7d/30d`. Сам endpoint только читает уже посчитанный результат, никогда не считает "на лету". |
| `GET /users/{login}/topics` | Считается **on-demand**, синхронно, один вызов LLM на весь набор сообщений пользователя (обычно десятки-сотни сообщений — влезает в контекст целиком). |
| Embeddings | Считаются **асинхронно** фоновым worker'ом сразу после вставки сообщения (не блокируют `POST /messages`, не нужны мгновенно — единственный потребитель, ночной job, ждёт часами). |
| `topic_score` | `norm(volume) + norm(unique_users)`, без учёта роста/динамики в v1. |
| RAM | Стек рассчитан на бюджет **≤4GB**. Ollama выгружает модели из памяти после простоя (`OLLAMA_KEEP_ALIVE`), поэтому embedding-модель и LLM не обязаны быть в памяти одновременно постоянно. |
| Идемпотентность / дедупликация | Не реализованы — `POST /messages` принимает сообщение как есть. |
| Backfill | Через тот же `POST /messages`, поштучно (см. `backfill/import_csv.py`) — отдельного bulk-API нет. |
| Auth / HTTPS | Не реализованы (внутренний инструмент, доверенная сеть). |

Полная история решений — в `user_message_topics_brief.md`.

## Установка

```bash
git clone <repo> topics-pulse   # или просто скопируйте эту папку
cd topics-pulse
cp .env.example .env
```

Откройте `.env` и при необходимости поменяйте: креды Postgres, модели Ollama (`OLLAMA_EMBEDDING_MODEL`, `OLLAMA_LLM_MODEL`), расписание ночного анализа (`ANALYSIS_CRON`), периоды (`ANALYSIS_PERIODS`) и т.д. Скачивать модели вручную не нужно — контейнер `ollama-init` подтянет их при первом старте автоматически.

## Запуск

```bash
docker compose up -d
```

Поднимаются 3 сервиса + одноразовый `ollama-init`:

1. `postgres` — применяет `postgres/init.sql` (схема, `pgvector`, `pg_trgm`, индексы) при первом старте.
2. `ollama` — модельный runtime.
3. `ollama-init` — качает `bge-m3` и LLM-модель в volume `ollama_data`, завершается и выходит (при повторных запусках — мгновенный no-op, модели уже скачаны).
4. `api` (Go) — HTTP-сервер + фоновый embedding-worker + ночной cron, стартует после того как `postgres` healthy и `ollama-init` завершился успешно.

Первый запуск может занять несколько минут (скачивание образов и моделей, несколько GB). Проверка:

```bash
docker compose ps
curl http://localhost:8080/health
```

## Взаимодействие

### Приём сообщения

Вызывается из игры / форума / telegram-бота один раз на каждое сообщение. Ответ мгновенный — просто `INSERT`, embedding считается в фоне.

```bash
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -d '{
    "login": "user123",
    "user_id": 987654,
    "text": "Не могу вывести деньги",
    "created_at": "2026-09-21T12:30:00Z",
    "source": "game"
  }'
```

Ответ:
```json
{"id": 42}
```

### Общие темы за период

Читает уже посчитанный ночью результат — период должен быть одним из `ANALYSIS_PERIODS` (по умолчанию `24h`, `7d`, `30d`).

```bash
curl "http://localhost:8080/topics?period=7d&limit=10"
```

Пример ответа:
```json
{
  "from": "2026-09-15T03:00:00Z",
  "to": "2026-09-22T03:00:00Z",
  "topics": [
    {
      "rank": 1,
      "name": "Проблемы с выводом средств",
      "summary": "Пользователи сообщают о задержках и зависших заявках на вывод.",
      "message_count": 431,
      "unique_users": 187,
      "sources": ["game", "telegram"],
      "representative_messages": [
        "Не могу вывести деньги",
        "Вывод уже сутки pending",
        "Почему не приходит выплата?"
      ],
      "topic_score": 1.72
    }
  ]
}
```

Если ночной job ещё не запускался для этого периода — `404`.

### Темы конкретного пользователя

Считается синхронно в момент запроса (ответ может занять несколько секунд — идёт вызов LLM).

```bash
curl "http://localhost:8080/users/user123/topics?from=2026-09-01T00:00:00Z&to=2026-09-08T00:00:00Z&limit=10"
```

### Полнотекстовый поиск

```bash
curl "http://localhost:8080/messages/search?q=вывод+денег&login=user123&limit=20"
```

Пример ответа:
```json
{
  "query": "вывод денег",
  "total": 2,
  "results": [
    {"id": 42, "login": "user123", "text": "Не могу вывести деньги", "created_at": "2026-09-21T12:30:00Z", "source": "game", "rank": 0.61},
    {"id": 57, "login": "user123", "text": "Вывод уже сутки pending", "created_at": "2026-09-21T13:10:00Z", "source": "telegram", "rank": 0.34}
  ]
}
```

### Ручной запуск анализа (для тестов, без ожидания ночи)

```bash
curl -X POST http://localhost:8080/internal/analyze
```

### Backfill исторических данных

```bash
python3 backfill/import_csv.py --file history.csv --api http://localhost:8080
```

Ожидаемые колонки CSV: `login,user_id,text,created_at,source` (см. docstring в самом скрипте). Отправляет сообщения по одному через тот же `POST /messages` — идемпотентности нет, повторный запуск создаст дубликаты.

### Интерактивная проверка API (OpenAPI / Swagger UI)

В проекте есть `openapi.yaml` (спецификация) и `openapi.html` (Swagger UI). Открывать `openapi.html` двойным кликом (`file://`) не будет работать — браузер блокирует загрузку соседнего `openapi.yaml` по CORS для локальных файлов. Поднимите любой статический сервер из корня проекта:

```bash
python3 -m http.server 8081
```

и откройте `http://localhost:8081/openapi.html` — API должен быть уже запущен (`docker compose up -d`), CORS на нём уже разрешён для локальных вызовов из браузера.

## Остановка

```bash
docker compose down       # данные остаются в volume
docker compose down -v    # полная очистка, включая скачанные модели
```

## Структура проекта

```text
topics-pulse/
├── docker-compose.yml
├── .env.example
├── README.md
├── openapi.yaml
├── openapi.html
├── postgres/
│   └── init.sql
├── api/
│   ├── Dockerfile
│   ├── go.mod / go.sum
│   ├── cmd/api/main.go
│   └── internal/{config,db,models,ollama,repository,service,handlers}/
└── backfill/
    └── import_csv.py
```
