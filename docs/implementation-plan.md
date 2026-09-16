# Implementation Plan — AI System Design Interviewer

**Status:** Draft v1
**Companion to:** [requirements-analysis.md](./requirements-analysis.md) — every task below is
traceable to a requirement ID (`SES-*`, `INT-*`, `PHZ-*`, `EVL-*`, `PER-*`, `HRN-*`) from that
document. If a task has no requirement ID, it's plumbing, and that's called out explicitly.
**Last updated:** 2026-09-13

---

## 1. How to use this document

This is the "how," not the "what." Requirements analysis already decided *what* the system
does and *why*; this document decides package layout, schemas, API contracts, prompt file
formats, provider integration details, and a task-level build order. Follow §9 (weekly task
list) top to bottom — each task references the section that specifies it.

---

## 2. Project structure

Single Spring Boot application (no microservices — one service, one worker process, both
built from the same codebase, run as two entry points). This matches NFR-7 (Kafka/Redis
should be swappable, not load-bearing architecture).

```
sd-ai-interviewer/
├── backend/
│   ├── pom.xml                          (Maven; Java 21, Spring Boot 3.x)
│   ├── src/main/java/dev/ahasanul/interviewer/
│   │   ├── InterviewerApplication.java
│   │   ├── config/
│   │   │   ├── LlmRolesConfig.java      (binds llm.* config → role→provider/model map)
│   │   │   ├── RedisConfig.java
│   │   │   ├── KafkaConfig.java
│   │   │   └── WebClientConfig.java
│   │   ├── problem/
│   │   │   ├── Problem.java, RubricItem.java (JPA entities)
│   │   │   ├── ProblemRepository.java, RubricItemRepository.java
│   │   │   └── ProblemController.java   (GET /api/problems, GET /api/problems/{id})
│   │   ├── session/
│   │   │   ├── Session.java, SessionStatus.java (JPA entity + enum)
│   │   │   ├── SessionRepository.java
│   │   │   ├── SessionService.java      (SES-1..4)
│   │   │   ├── SessionState.java        (Redis-backed live state, not JPA)
│   │   │   └── SessionController.java   (POST/GET /api/sessions/*)
│   │   ├── turn/
│   │   │   ├── Turn.java, TurnRole.java
│   │   │   ├── TurnRepository.java
│   │   │   ├── TurnService.java         (INT-1..4)
│   │   │   └── TurnController.java      (POST /api/sessions/{id}/turns, SSE)
│   │   ├── phase/
│   │   │   ├── Phase.java, PhaseChecklistItem.java
│   │   │   └── PhaseControllerService.java (PHZ-1..4)
│   │   ├── evaluation/
│   │   │   ├── Evaluation.java, EvaluationStatus.java
│   │   │   ├── EvaluationRepository.java
│   │   │   ├── EvaluationRequestProducer.java   (EVL-1, Kafka producer)
│   │   │   ├── EvaluationWorker.java            (EVL-2, Kafka @KafkaListener)
│   │   │   ├── EvaluatorService.java            (EVL-3..7, batched scoring)
│   │   │   └── ReportController.java            (GET /api/sessions/{id}/report)
│   │   ├── llm/
│   │   │   ├── LlmClient.java            (interface: streamChat, completeJson)
│   │   │   ├── LlmRequest.java, LlmResponse.java, LlmUsage.java (DTOs)
│   │   │   ├── gemini/GeminiClient.java
│   │   │   ├── groq/GroqClient.java
│   │   │   ├── anthropic/AnthropicClient.java   (evaluator fallback only, §6.1 of requirements)
│   │   │   └── LlmClientFactory.java     (resolves role → client, from LlmRolesConfig)
│   │   ├── prompt/
│   │   │   ├── PromptTemplate.java
│   │   │   ├── PromptService.java        (loads + renders versioned prompt files, PER-1/2)
│   │   │   └── PromptVersion.java (JPA entity)
│   │   ├── context/
│   │   │   └── ContextCompressionService.java   (CTX-1, CTX-2)
│   │   ├── cost/
│   │   │   └── CostTrackingService.java  (PER-3)
│   │   └── ratelimit/
│   │       └── ProviderRateLimiter.java  (NFR-9, Redis-backed token bucket)
│   ├── src/main/resources/
│   │   ├── application.yml
│   │   ├── db/migration/                 (Flyway: V1__init.sql, V2__..., see §4)
│   │   └── prompts/
│   │       ├── interviewer/v1.txt
│   │       ├── phase-controller/v1.txt
│   │       └── evaluator/v1.txt
│   └── src/test/java/dev/ahasanul/interviewer/...  (mirrors main; see §8)
├── frontend/
│   └── (Angular app — see §7)
├── harness/
│   └── (eval harness scripts — see §6)
├── docker-compose.yml                    (Postgres, Redis, Kafka — see §3)
└── docs/
    ├── requirements-analysis.md
    └── implementation-plan.md            (this file)
```

---

## 3. Local development environment

**Postgres runs natively** on this machine (PostgreSQL 18 as a Windows service), not via
Docker — it was already installed and running before this project started, so there's no
reason to containerize it too. The `interviewer` database and role are already created.

**Redis and Kafka run via Docker Compose** (`docker-compose.yml` at repo root), using WSL2 as
the backend (see the ADR-style note at the end of this section for why Redis/Kafka specifically
needed this and Postgres didn't):

```yaml
services:
  redis:
    image: redis:7
    ports: ["6379:6379"]

  kafka:
    image: apache/kafka:3.7.0        # KRaft mode, no separate Zookeeper needed
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
```

Why Postgres didn't need Docker/WSL2 but Redis and Kafka did: Postgres ships an actively
maintained native Windows installer (EnterpriseDB). Redis's maintainers only build for
Linux/macOS — no supported native Windows build exists. Kafka is JVM-based and technically
has Windows `.bat` scripts, but is known to be unreliable on Windows (NTFS file-locking
issues), so nobody runs it that way in practice. Docker (backed by WSL2, since this machine
had virtualization disabled in BIOS/firmware until it was enabled for this project) gives
both a real Linux environment to run in, which also happens to mirror how these services
actually run in production almost everywhere.

Environment variables (`.env`, gitignored; `.env.example` committed — NFR-8):

```
GEMINI_API_KEY=
GROQ_API_KEY=
ANTHROPIC_API_KEY=            # optional, evaluator fallback only
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/interviewer
SPRING_DATASOURCE_USERNAME=interviewer
SPRING_DATASOURCE_PASSWORD=interviewer
SPRING_REDIS_HOST=localhost
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Getting free API keys (do this first, it's the actual first task):
1. Gemini: https://aistudio.google.com/apikey — free tier, no card required at time of writing.
2. Groq: https://console.groq.com/keys — free tier, no card required at time of writing.
3. Verify both with a raw `curl` before writing any Java code (fastest way to confirm the
   account/key actually works and to see real response shapes).

---

## 4. Database schema (Flyway `V1__init.sql`)

Expands requirements §7 with real types/constraints.

```sql
CREATE TABLE problems (
    id          BIGSERIAL PRIMARY KEY,
    title       VARCHAR(200) NOT NULL,
    statement   TEXT NOT NULL,
    difficulty  VARCHAR(20) NOT NULL CHECK (difficulty IN ('EASY','MEDIUM','HARD')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE phases (
    id          BIGSERIAL PRIMARY KEY,
    problem_id  BIGINT NOT NULL REFERENCES problems(id) ON DELETE CASCADE,
    name        VARCHAR(100) NOT NULL,          -- e.g. 'requirements', 'estimation'
    seq         INT NOT NULL,                   -- ordering within a problem
    UNIQUE (problem_id, seq)
);

CREATE TABLE rubric_items (
    id              BIGSERIAL PRIMARY KEY,
    problem_id      BIGINT NOT NULL REFERENCES problems(id) ON DELETE CASCADE,
    phase_id        BIGINT REFERENCES phases(id),
    criterion       VARCHAR(300) NOT NULL,
    weight          NUMERIC(4,2) NOT NULL DEFAULT 1.0,
    reference_notes TEXT NOT NULL               -- what a strong answer looks like (EVL-5)
);

CREATE TABLE prompt_versions (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(50) NOT NULL,           -- 'interviewer' | 'phase-controller' | 'evaluator'
    version     INT NOT NULL,
    body        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE TABLE sessions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    problem_id  BIGINT NOT NULL REFERENCES problems(id),
    status      VARCHAR(20) NOT NULL DEFAULT 'IN_PROGRESS'
                    CHECK (status IN ('IN_PROGRESS','EVALUATING','COMPLETED')),
    current_phase_id BIGINT REFERENCES phases(id),
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at    TIMESTAMPTZ
);

CREATE TABLE turns (
    id              BIGSERIAL PRIMARY KEY,
    session_id      UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    seq             INT NOT NULL,
    role            VARCHAR(20) NOT NULL CHECK (role IN ('CANDIDATE','INTERVIEWER')),
    content         TEXT NOT NULL,
    phase_id        BIGINT REFERENCES phases(id),
    tokens_in       INT,
    tokens_out      INT,
    latency_ms      INT,
    cost_usd        NUMERIC(10,6),
    prompt_version_id BIGINT REFERENCES prompt_versions(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, seq)
);

CREATE TABLE evaluations (
    id              BIGSERIAL PRIMARY KEY,
    session_id      UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    rubric_item_id  BIGINT NOT NULL REFERENCES rubric_items(id),
    status          VARCHAR(20) NOT NULL CHECK (status IN ('MET','PARTIAL','ABSENT')),
    evidence        TEXT,                       -- required unless status = 'ABSENT'
    gap             TEXT,
    prompt_version_id BIGINT REFERENCES prompt_versions(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, rubric_item_id, prompt_version_id) -- allows re-eval under a new prompt version (HRN-4)
);

-- Application-layer invariant (EVL-4), enforced in EvaluatorService, documented here since
-- Postgres CHECK constraints can't easily express "evidence required unless ABSENT":
-- INSERT/UPDATE must reject a non-ABSENT evaluation row with NULL/empty evidence.

CREATE INDEX idx_turns_session ON turns(session_id);
CREATE INDEX idx_evaluations_session ON evaluations(session_id);
```

Seed data (`V2__seed_problems.sql`): 3 problems at launch, not 1, so the eval harness (§6)
has variety — e.g. **rate limiter**, **URL shortener**, **notification/fan-out system**. Each
needs 12–20 rubric items split across phases per requirements §HRN-1 note about rubric
variety.

---

## 5. LLM integration layer

### 5.1 Common interface

```java
public interface LlmClient {
    Flux<String> streamChat(LlmRequest request);       // interviewer role
    Mono<LlmResponse> completeJson(LlmRequest request); // phase controller + evaluator roles
}

public record LlmRequest(
    String systemPrompt,
    List<ChatMessage> messages,
    Map<String, Object> jsonSchema,   // null for streamChat
    double temperature
) {}

public record LlmResponse(
    String rawText,
    JsonNode parsedJson,       // null for streamChat path
    int tokensIn,
    int tokensOut,
    int latencyMs
) {}
```

### 5.2 Provider clients

| Client | Underlying API | Notes |
|---|---|---|
| `GeminiClient` | `generateContent` (streaming via `streamGenerateContent`, SSE) | Use `responseSchema` for JSON mode (phase controller / evaluator) — native structured output support avoids brittle prompt-based JSON coaxing. |
| `GroqClient` | OpenAI-compatible `/chat/completions`, `stream: true` | Use `response_format: {type: "json_object"}` for structured calls. |
| `AnthropicClient` | Messages API | Evaluator fallback only (§6.1 of requirements) — not wired into default config, added when/if the harness shows free-tier evaluator accuracy is insufficient. |

All three implemented with Spring `WebClient` (reactive, natural fit for SSE streaming into
the interviewer endpoint). No LangChain, no generic multi-provider SDK (per requirements
§12).

### 5.3 Role → provider binding (`application.yml`)

```yaml
llm:
  roles:
    interviewer:
      provider: gemini
      model: gemini-2.0-flash
    phase-controller:
      provider: groq
      model: llama-3.1-8b-instant
    evaluator:
      provider: gemini
      model: gemini-2.0-flash
      fallback:                 # only used if explicitly enabled per requirements §6.1
        provider: anthropic
        model: claude-3-5-haiku
        enabled: false
```

`LlmClientFactory.forRole(Role role)` reads this config and returns the bound `LlmClient`.
Swapping a role to a different provider is a one-line config change, never a code change —
this is what makes the "not over-built for one provider" requirement (NFR from §12 discussion)
concrete rather than aspirational.

### 5.4 Rate limiting (NFR-9)

`ProviderRateLimiter` — before every outbound call, increment a Redis counter keyed
`ratelimit:{provider}:{yyyyMMddHHmm}` with a 60s TTL; if it exceeds the provider's published
free-tier RPM, queue/delay client-side (simple `Mono.delay` backoff) rather than firing and
eating a 429. This is a self-imposed ceiling, not a retry-after-failure strategy — cheaper
and simpler than handling provider 429s after the fact for a single-user app.

---

## 6. Prompts and the eval harness

### 6.1 Prompt file format

`src/main/resources/prompts/{role}/v{n}.txt` — plain text with `{{placeholder}}` tokens,
rendered by `PromptService` using Apache Commons Text `StringSubstitutor` (no templating
engine needed for this). On app startup, each file's contents are upserted into
`prompt_versions` (name, version, body) so `turns.prompt_version_id` /
`evaluations.prompt_version_id` always point at the exact text that produced them (PER-1/2).

Example skeleton, `prompts/evaluator/v1.txt`. Corrected to match EVL-3 (batches of ~4 items
per call, not one) — the input is an array of rubric items, the output an array of results,
one per input item, each still evidence-first:

```
You are scoring a system design interview transcript against a fixed rubric.

RUBRIC ITEMS TO SCORE (score each independently; do not let one item's evidence justify another):
{{#rubric_items}}
- id: {{id}}
  criterion: {{criterion}}
  what a strong answer looks like: {{reference_notes}}
{{/rubric_items}}

TRANSCRIPT:
{{transcript}}

Respond with a JSON array with exactly one object per rubric item above, in this shape:
[
  {
    "rubric_item_id": <id>,
    "evidence": "<verbatim quote from the transcript, or null if none exists>",
    "status": "met" | "partial" | "absent",
    "gap": "<what's missing, or null if met>"
  }
]

Rules:
- Produce "evidence" first, as a literal substring of the transcript above, before deciding status.
- If you cannot find a supporting quote for an item, its status MUST be "absent" and evidence MUST be null.
- Do not infer intent the candidate did not state. Silence on a topic is absent, not partial.
- Return exactly one result object per rubric item id given, in the same order.
```

Note: `{{#rubric_items}}...{{/rubric_items}}` is a section loop, which `StringSubstitutor`
(§6.1 above) can't do — this one prompt needs an actual small templating engine (e.g.
Mustache/JMustache) rather than flat placeholder substitution. Everywhere else (interviewer,
phase-controller prompts) stays flat placeholders; don't upgrade those without a reason.

This directly implements EVL-4 (evidence-before-verdict) at the prompt level, and EVL-3
(batched, not one-shot-per-item) at the schema level. `EvaluatorService` enforces evidence
again at the code level per result in the array (reject/mark-absent any element where
`evidence` is null/blank but `status` != `absent`) — belt and suspenders, since this
constraint is the entire point of the evaluator design. It also validates the response array
length/id-set matches the batch it sent; a provider that drops or duplicates an item is a
hard failure for that batch, not a silently incomplete evaluation.

### 6.2 Eval harness (`/harness`)

A small standalone module (plain Java or even a script — doesn't need to be a Spring app)
implementing HRN-1..5:

```
harness/
├── sessions/                    # 30 real transcripts, exported from Postgres as JSON
├── labels/                      # hand-graded ground truth, one file per session,
│                                 #   same shape as an `evaluations` row
├── replay.sh                    # republishes session ids to the Kafka topic for re-scoring
└── agreement_report.py          # compares evaluations table vs labels/, per rubric item
```

`agreement_report.py` output — per rubric item, not aggregate (HRN-3):

```
rubric_item_id | criterion                          | agreement | evaluator_over | evaluator_under
14             | addressed hot key problem          | 0.83      | 2              | 3
7              | provided capacity/QPS estimate      | 0.60      | 5              | 1   <-- flag
```

Any row where `evaluator_over` is high on a criterion the harness deliberately left out
(HRN-5's planted failure case: "skip capacity estimation entirely") is the signal to fix the
evaluator prompt, per requirements §9 success criterion 2.

---

## 7. Frontend (Angular)

Minimal, function-first — this project's differentiation is backend/LLM architecture, not UI
polish.

```
frontend/src/app/
├── problem-list/         # GET /api/problems → pick a problem, POST /api/sessions
├── interview/            # main screen: transcript view + input box
│   ├── interview.component.ts     # holds session id, phase, transcript array
│   └── sse.service.ts             # wraps EventSource for POST-then-stream turn responses
├── report/               # GET /api/sessions/{id}/report → per-rubric-item breakdown
└── core/
    └── api.service.ts    # typed HttpClient wrappers for all non-streaming endpoints
```

SSE note: browser `EventSource` only supports GET. Since turn submission is logically a POST
(it has a body), implement as: `POST /api/sessions/{id}/turns` kicks off processing and
returns a `streamId`; client then opens `EventSource` against
`GET /api/sessions/{id}/turns/{streamId}/stream`. Document this explicitly so it isn't
"discovered" mid-Week-3 — it's a known REST+SSE gotcha, not a design flaw.

---

## 8. API contracts (concrete)

| Method & path | Request body | Response | Req ID |
|---|---|---|---|
| `GET /api/problems` | — | `[{id, title, difficulty}]` | — |
| `GET /api/problems/{id}` | — | `{id, title, statement, difficulty, phases:[{name, seq}]}` | — |
| `POST /api/sessions` | `{problemId}` | `{sessionId, problem, currentPhase}` | SES-1/2 |
| `POST /api/sessions/{id}/turns` | `{content}` | `202 {streamId}` | INT-1 |
| `GET /api/sessions/{id}/turns/{streamId}/stream` | — (SSE) | `event: token` chunks, `event: done` with `{phaseAdvanced: bool, currentPhase}` | INT-2, PHZ-3 |
| `POST /api/sessions/{id}/end` | — | `202 {status:"EVALUATING"}` | SES-4, EVL-1 |
| `GET /api/sessions/{id}` | — | `{status, currentPhase, transcript:[...]}` | SES-3 |
| `GET /api/sessions/{id}/report` | — | `{overallScore, items:[{criterion, status, evidence, gap, weight}]}` | EVL-7 |

---

## 9. Task-level build order

Expands requirements §11 into checkable tasks. Each week ends with a working demoable
slice — do not start next week's tasks with prior week's slice broken.

### Week 1 — prove the core loop
- [ ] Get Gemini + Groq API keys; verify both with raw `curl` (§3).
- [ ] Scaffold Spring Boot project (Maven, Java 21, Web + WebFlux + JPA + Postgres driver).
- [ ] `docker-compose up postgres` only (skip Redis/Kafka this week entirely).
- [ ] Flyway `V1__init.sql` (§4), `V2__seed_problems.sql` with **1** problem (rate limiter)
      and its rubric items.
- [ ] `LlmClient` interface + `GeminiClient` (streamChat only, no JSON mode yet).
- [ ] `POST /api/sessions` (SES-1/2, no Redis — just a Postgres row and an in-memory map for
      "current phase," phases aren't enforced yet).
- [ ] `POST /api/sessions/{id}/turns`, **non-streaming** single-shot call to Gemini, returns
      the full response as JSON (no SSE yet — that's Week 3, INT-2).
- [ ] Manual end-to-end test: start a session, send 2–3 turns, see interviewer replies.
- **Definition of done:** you can `curl` your way through a full back-and-forth with a real
  LLM and see it persisted in `turns`.

### Week 2 — transcript persistence, rubric, evaluator, first labels
- [ ] `turns` table fully wired (tokens_in/out, latency_ms, cost_usd — PER-3); add a
      `CostTrackingService` that computes cost from token counts using each provider's
      published free-tier/paid pricing (0 for free tier, but keep the field so paid fallback
      is a config flip, not new code).
- [ ] `prompt_versions` table + `PromptService` loading `prompts/*/v1.txt` on startup (§6.1).
- [ ] Write `prompts/evaluator/v1.txt` per the skeleton in §6.1.
- [ ] `EvaluatorService`: batches of 4 rubric items per call (EVL-3), calls
      `LlmClient.completeJson`, enforces evidence-before-verdict at the code level (EVL-4).
- [ ] `evaluations` table wired; run evaluation **synchronously** for now (no Kafka yet — call
      `EvaluatorService` directly at end-of-session). Async is Week 4.
- [ ] `GET /api/sessions/{id}/report`.
- [ ] Play through **10 real sessions** yourself (mix of genuine and deliberately bad, per
      HRN-1); hand-label each into `harness/labels/`.
- **Definition of done:** a session produces a real per-rubric-item report with quotes, and
  you have 10 hand-labeled sessions on disk.

### Week 3 — streaming, UI, Redis, phase control
- [ ] Convert `POST /api/sessions/{id}/turns` to the two-step SSE pattern (§7 note): kick off
      + `GET .../stream` (INT-2).
- [ ] `docker-compose up redis` too now.
- [ ] `SessionState` (Redis hash: phase, turn count, last-activity) — reads that would've hit
      Postgres now hit Redis (per requirements' stated reason for Redis).
- [ ] Add remaining 2 problems + their phases (§4 seed data) so phase transitions are
      meaningful, not single-phase.
- [ ] `prompts/phase-controller/v1.txt` + `PhaseControllerService` using `GroqClient`
      (PHZ-1..4); wire into the turn flow (runs after each candidate turn, per PHZ-2).
- [ ] `ProviderRateLimiter` (NFR-9) — implement now since Groq calls per-turn make free-tier
      RPM limits realistic to hit.
- [ ] Angular app: problem list → interview screen (SSE-consuming) → basic report view.
- **Definition of done:** a full multi-phase interview runs in the browser, phase advances
  automatically, and you can watch tokens stream in.

### Week 4 — Kafka, async evaluation, prompt versioning discipline
- [ ] `docker-compose up kafka` too; create topic `evaluation-requested`.
- [ ] `EvaluationRequestProducer` (EVL-1): `POST /api/sessions/{id}/end` now just publishes
      `{sessionId}` and returns immediately, status flips to `EVALUATING`.
- [ ] `EvaluationWorker` (`@KafkaListener`, EVL-2): consumes, runs the same `EvaluatorService`
      from Week 2, writes results, flips status to `COMPLETED`.
- [ ] `ContextCompressionService` (CTX-1/2): summarize completed phases once transcript
      length crosses a token threshold; verify evaluator input path bypasses this (CTX-2) —
      write an explicit test for that bypass, it's easy to accidentally wire compression
      globally.
- [ ] Redis LLM response cache keyed on `sha256(role + model + rendered-prompt)`, dev-profile
      only, protects free-tier quota during iteration (§6.1 of requirements).
- **Definition of done:** ending a session returns instantly; the report appears 10–30s
  later without blocking anything; killing/restarting the worker mid-evaluation doesn't lose
  the request (Kafka consumer offset handles this for free, per requirements).

### Week 5+ — harness-driven iteration
- [ ] Finish the 30-session harness set (20 more beyond Week 2's 10; include deliberately bad
      ones per HRN-1).
- [ ] `harness/agreement_report.py` (§6.2) — per-rubric-item agreement, not aggregate.
- [ ] Run it against `evaluator/v1.txt`; find at least one concrete over-crediting bug
      (requirements §9, success criterion 2) — expect this, don't be surprised by it.
- [ ] Write `evaluator/v2.txt` fixing that bug; re-run harness via `replay.sh`; compare
      agreement numbers v1 vs v2 (this comparison, saved somewhere durable, is itself a
      portfolio artifact — screenshot or keep the CSV).
- [ ] Repeat until per-item agreement is at a level you're prepared to defend in an
      interview, not until it "feels good."
- [ ] Only now, if free-tier evaluator agreement is still unacceptable after prompt
      iteration: enable the Anthropic fallback (§5.3, `enabled: true`) and re-run the harness
      comparison free vs. paid — that comparison is a legitimate finding worth writing up
      regardless of which way it goes.

---

## 10. Testing strategy

- **Unit tests:** `PromptService` rendering, `EvaluatorService`'s evidence-enforcement logic
  (this one deserves real test coverage — it's the core correctness property of the whole
  project), `PhaseControllerService` JSON parsing edge cases.
- **Integration tests:** Testcontainers for Postgres/Redis/Kafka in CI; **mock the LLM
  clients** (a `FakeLlmClient` returning canned responses) so tests don't burn free-tier
  quota or depend on network — this also lets you unit-test the evidence-enforcement
  invariant with a fake response that violates it, to prove the guard actually rejects bad
  output.
- **Harness runs are not CI-gated** — they're a manual/scheduled research activity (§9 Week
  5+), not a pass/fail test suite; agreement thresholds are judgment calls you make and
  record, not asserted in code.

---

## 11. Open implementation decisions (resolve when reached, not now)

- Exact free-tier RPM/RPD numbers for Gemini and Groq change over time — read current limits
  from each provider's docs when implementing `ProviderRateLimiter` in Week 3, don't hardcode
  numbers from this doc.
- Whether `phases` are literally per-problem rows (as modeled in §4) or a shared fixed
  enum — modeled as per-problem rows here since different problems may reasonably want
  different phase sets, but a fixed global enum is simpler if that flexibility never gets
  used. Decide in Week 3 once 3 problems' phase lists are actually written out.
