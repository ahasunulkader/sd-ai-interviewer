# Requirements Analysis — AI System Design Interviewer

**Status:** Draft v1
**Owner:** Ahasanul Kader
**Last updated:** 2026-09-13

---

## 1. Purpose

This document defines the requirements for an AI-powered System Design Interview practice
platform. The application simulates a technical interviewer for system design questions
(e.g. "design a rate limiter", "design a URL shortener"), conducts a structured, multi-phase
conversation with the candidate, and produces an evidence-based evaluation report at the end
of the session.

### 1.1 Primary goal (why this project exists)

This is a **learning project**, not a commercial product. The explicit objective is to gain
hands-on, defensible experience with:

- Designing and operating a system that integrates **multiple LLM calls with different
  roles, different models, and different cost/latency/quality tradeoffs** in one product.
- Building an **evaluation harness** for LLM output quality — the hardest and most
  differentiated part of any LLM-backed product, and the part most candidates skip.
- Justifying architecture decisions (Kafka, Redis, async workers) under interview-style
  follow-up questioning, including being honest about which decisions were made for
  learning/resume purposes rather than necessity.

This goal directly shapes several requirements below: the project deliberately includes
infrastructure (Kafka, Redis, prompt versioning, an eval suite) that a single-user tool does
not strictly need, because building and being able to explain that infrastructure **is** the
point.

### 1.2 Secondary goal

Produce a genuinely useful personal tool for practicing system design interviews, with
feedback that is specific and evidence-backed rather than generic praise.

---

## 2. Scope

### 2.1 In scope (v1)

- Single-user application (no multi-tenant auth/billing in v1).
- A fixed library of hardcoded system design problems, each with a rubric.
- A conversational interview flow driven by an LLM, structured into phases.
- Automatic phase progression via a lightweight LLM-based phase controller.
- Streaming responses to the UI (SSE).
- Asynchronous, rubric-based evaluation at the end of a session, producing a structured
  report (not a single score).
- Full persistence of transcripts, turns, phase transitions, and evaluation results.
- Cost and latency tracking per turn and per session.
- Prompt versioning, so scoring changes can be attributed to specific prompt changes.
- An offline eval harness with hand-labeled sessions to measure evaluator accuracy.
- Ability to re-run evaluation for past sessions against a new prompt version (regression
  testing for prompts).

### 2.2 Out of scope (v1)

- Multi-user accounts, auth, teams, sharing.
- Payment/billing.
- Mobile app (web only).
- Voice input/output.
- Support for arbitrary/free-text problem submission (problems are curated, not
  user-authored).
- Human-in-the-loop review UI for evaluations (may be added later to build more labeled
  data).
- Fine-tuning any model. All model behavior is via prompting.
- High availability / multi-region deployment. This runs as a single-instance learning
  project, not a production SLA-backed service.

### 2.3 Explicitly acknowledged over-engineering

Kafka and Redis are **not required** at this scale (single user). They are included because
practicing the async-worker pattern and the cache/session-state pattern is a stated goal
(§1.1). This tradeoff must be stated plainly if raised in an interview — see §10.

---

## 3. Actors

| Actor | Description |
|---|---|
| Candidate | The user practicing an interview. In v1, this is the project owner. |
| Interviewer (LLM role) | Conversational agent that asks questions, probes gaps, never gives away answers or empty praise. |
| Phase Controller (LLM role) | Silent, structured-output agent that decides when a phase's checklist is satisfied. |
| Evaluator (LLM role) | Runs once per session, post-hoc, scores against a rubric with evidence. |
| Eval Harness Operator | The developer, running hand-labeled sessions through the evaluator to measure and improve its accuracy. |

---

## 4. Functional Requirements

Requirement IDs are prefixed by area: `SES` (session), `INT` (interviewer turn),
`PHZ` (phase control), `EVL` (evaluation), `PER` (persistence), `HRN` (eval harness).

### 4.1 Session lifecycle

- **SES-1**: User can select a problem from a fixed list and start a new session.
- **SES-2**: Starting a session creates a `sessions` row, loads the problem statement and its
  rubric items, initializes session state in Redis, and returns a session id to the client.
- **SES-3**: A session has a status: `in_progress`, `evaluating`, `completed`.
- **SES-4**: User can end a session explicitly, or it ends automatically after a configurable
  max turn count / time limit (to bound cost).

### 4.2 Interview turn

- **INT-1**: On each candidate message, the API appends the turn to the transcript, builds a
  prompt from problem context + phase + recent transcript (+ compressed summary of earlier
  phases, see §4.5), and calls the interviewer model.
- **INT-2**: Interviewer responses stream to the client via SSE, token by token.
- **INT-3**: The interviewer prompt enforces: never reveal the ideal solution, never give
  empty/generic praise ("great job"), always probe when a candidate skips a required
  consideration for the current phase (e.g., capacity estimation).
- **INT-4**: On stream completion, the full turn (input + output + token counts + latency +
  cost + phase at time of turn) is persisted to Postgres. Redis holds only live/session-scoped
  state, not the durable record.

### 4.3 Phase control

- **PHZ-1**: Each problem defines an ordered list of phases (e.g. requirements →
  capacity estimation → high-level design → deep dive) with a checklist of items per phase.
- **PHZ-2**: After each candidate turn, a phase-controller call determines whether the
  current phase's checklist is satisfied, returning structured JSON:
  `{"phase_complete": bool, "missing": string[]}`.
- **PHZ-3**: If `phase_complete` is true, the orchestrator advances the session to the next
  phase and the interviewer is informed of the new phase on the next turn.
- **PHZ-4**: Phase control uses the cheapest available model since the task is a narrow
  classification/extraction task, not open-ended generation.

### 4.4 Evaluation

- **EVL-1**: When a session ends, the API publishes `{session_id}` to a Kafka topic and
  immediately returns; the UI shows an "evaluating" state.
- **EVL-2**: A worker consumes the message, loads the full transcript and the problem's
  rubric items, and runs the evaluation pass.
- **EVL-3**: Rubric items are scored in small batches (e.g. 4 items per LLM call), not as one
  pass over the whole rubric, to keep each call focused and reduce hallucinated agreement.
- **EVL-4**: The output schema for each rubric item requires, in order: (1) a verbatim quote
  from the transcript as evidence, then (2) a status
  (`met` / `partial` / `absent`), then (3) a gap description if not fully met. If no quote can
  be produced, status must be `absent`. The evidence-before-verdict ordering is a hard
  constraint, not a suggestion — it is the mechanism that suppresses ungrounded praise.
- **EVL-5**: Each rubric item's evaluation is given reference notes (what a strong answer
  looks like) as grounding context, not scored in the abstract.
- **EVL-6**: The evaluator prompt contains no signal identifying which model conducted the
  interview, and is not told it is grading a conversation it participated in (it never did —
  interviewer and evaluator are always different calls/models — but the prompt must not leak
  any framing that invites leniency).
- **EVL-7**: Final report aggregates per-item results into a session-level report (weighted
  by rubric item weight) and is persisted before the session status flips to `completed`.
- **EVL-8**: Typical evaluation turnaround is 10–30 seconds from end-of-session to report
  availability.

### 4.5 Context compression

- **CTX-1**: When a session's transcript approaches the interviewer model's context window,
  completed phases are summarized to a few lines each; only the current and immediately
  prior phase are kept verbatim.
- **CTX-2**: Compression must not run on the evaluator's input — the evaluator always sees
  the **full, uncompressed transcript**, since evidence quotes must be verifiable against the
  real conversation.

### 4.6 Persistence & traceability

- **PER-1**: Every turn records which prompt version produced it.
- **PER-2**: Every evaluation record references the `prompt_version` used to produce it, so
  a scoring change can be attributed to a specific prompt change.
- **PER-3**: Cost (`tokens_in`, `tokens_out`, `cost`) and `latency_ms` are recorded per turn,
  enabling per-session and aggregate cost reporting.

### 4.7 Eval harness

- **HRN-1**: A set of at least 30 sessions is collected: some genuinely attempted, some
  deliberately bad (skip requirements, wrong database choice, no scaling discussion).
- **HRN-2**: Each of the 30 is hand-labeled by the developer against the rubric, stored as
  ground truth.
- **HRN-3**: A harness script runs the evaluator against all 30 and computes agreement
  between evaluator output and hand labels, **per rubric item**, not just an aggregate score.
- **HRN-4**: The harness supports re-running the full 30-session set against a new prompt
  version (via republishing session ids to the same Kafka topic) so prompt changes can be
  measured before being adopted, not just eyeballed.
- **HRN-5**: A known target failure mode to explicitly test for: the evaluator awarding a
  high score on an item like "provided capacity estimation" when no such estimation appears
  anywhere in the transcript. This is the canonical case that HRN-3/EVL-4 exist to catch.

---

## 5. Non-Functional Requirements

| ID | Requirement |
|---|---|
| NFR-1 | Interviewer turn: first streamed token should appear within ~1–2s of submission (perceived responsiveness matters more than raw model speed here). |
| NFR-2 | Phase controller call should not noticeably delay the interviewer's response; run it in parallel with, or immediately after, streaming starts — not blocking the user-visible turn. |
| NFR-3 | Evaluation must complete asynchronously; the ending-session request must return in well under 1s regardless of evaluation duration. |
| NFR-4 | All LLM calls must be retryable (idempotent on the consumer side) to survive provider 429s/5xxs without operator intervention. |
| NFR-5 | Every LLM call's model, prompt version, token counts, and cost must be attributable after the fact — no unattributed spend. |
| NFR-6 | Prompts live as versioned files in source control (`src/main/resources/prompts/`), not as strings inline in code, so prompt history is diffable. |
| NFR-7 | The system must degrade to a usable (if less scalable) form without Kafka/Redis — i.e., the async boundary and cache boundary should be clean enough that swapping Kafka for an in-process queue or Redis for an in-memory map is a contained change, since v1 is single-user. |
| NFR-8 | No secrets (API keys) committed to source control; loaded via environment/config. |
| NFR-9 | System must stay within free-tier rate limits (requests/minute, requests/day) of the chosen providers (§6.1) under normal single-user usage; Redis response caching and self rate-limiting exist specifically to enforce this. |

---

## 6. High-Level Architecture

```
                     ┌─────────────┐
   candidate ───────▶│  Spring API │──────▶ Postgres (durable: problems, rubric,
                     └──────┬──────┘                   sessions, turns, evaluations,
                            │  SSE stream                  prompt_versions)
                            ▼
                     ┌─────────────┐
                     │    Redis    │  live session state, LLM response cache,
                     └─────────────┘  self rate-limiting
                            │
                            │ on session end: publish {session_id}
                            ▼
                     ┌─────────────┐
                     │    Kafka    │  evaluation-requested topic
                     └──────┬──────┘
                            ▼
                     ┌─────────────┐
                     │   Worker    │──────▶ runs Evaluator (batched, evidence-first)
                     └─────────────┘        writes evaluations → Postgres
```

Three distinct LLM roles, three distinct call shapes — this separation is the core design
decision and must never be collapsed into one call:

| Role | Cadence | Latency need | Output | Suggested provider/model (v1, cost-driven) |
|---|---|---|---|---|
| Interviewer | Every turn | Low (streamed) | Free text | Google Gemini free tier (e.g. Gemini 2.x Flash) or Groq free tier (Llama) |
| Phase Controller | Every turn | Low, non-blocking | Small JSON | Groq free tier — fastest, cheapest, task is narrow classification |
| Evaluator | Once per session | None (async) | Structured, evidence-required JSON, batched | Gemini free tier by default; swap in a small paid model (e.g. Claude Haiku / GPT-4o-mini class, cents/session) only if harness agreement (§HRN-3) is unacceptably low on the free tier |

### 6.1 Cost policy

Explicit project constraint: **target $0 spend**, since this is a learning project, not a
funded one. Practical implications:

- All three roles default to a free-tier provider (Google Gemini AI Studio and/or Groq).
  Both currently offer genuinely free API access, not just trial credits, though free tiers
  are rate-limited (requests/minute and requests/day) — acceptable for a single-user project.
- The interviewer and phase controller are expected to work well on free-tier models; these
  are lower-stakes, higher-frequency calls where model capability is not the bottleneck.
- The evaluator is the one place quality risk is real (see §9, success criteria) — if the
  free-tier model fails to catch the "flattery despite missing content" failure mode on the
  harness (§HRN-5), the fallback is a **small, paid, cheap** model for the evaluator only
  (evaluator runs once per session, not per turn, so this stays inexpensive even if not
  free). This should be treated as an explicit escalation decision recorded in
  `prompt_versions`/harness notes, not a default.
- Provider abstraction should not be over-built (no need for a generic multi-provider
  framework, see LangChain exclusion in §12) — a thin call wrapper per role is enough to swap
  providers/models without a rewrite.
- Redis prompt-hash caching (§ Redis, infra table) matters more under this policy, not less:
  it keeps free-tier rate limits from being burned by repeated dev-time replays of the same
  session during prompt iteration.

---

## 7. Data Model

```
problems        (id, title, statement, difficulty)
rubric_items    (id, problem_id, phase, criterion, weight, reference_notes)
sessions        (id, problem_id, status, started_at, ended_at)
turns           (id, session_id, seq, role, content, phase,
                  tokens_in, tokens_out, latency_ms, cost, prompt_version)
evaluations     (id, session_id, rubric_item_id, status, evidence, gap, prompt_version)
prompt_versions (id, name, version, body, created_at)
```

Notes:

- `turns.prompt_version` and `evaluations.prompt_version` are what make "did my prompt change
  help" an answerable question (§HRN-4) rather than a guess.
- `evaluations.evidence` stores the required quote (EVL-4); an evaluation row with a null/empty
  evidence field and a non-`absent` status is a data-integrity bug, not a valid state, and
  should be rejected at the application layer.
- Full transcripts (`turns.content`) are a first-class asset for building future eval data,
  not a log to be truncated or rotated out.

---

## 8. API Surface (indicative, v1)

| Endpoint | Purpose |
|---|---|
| `GET /problems` | List available problems. |
| `POST /sessions` | Start a session for a given problem id. |
| `POST /sessions/{id}/turns` (SSE) | Submit a candidate turn; stream interviewer response. |
| `POST /sessions/{id}/end` | End session, trigger async evaluation, return immediately. |
| `GET /sessions/{id}` | Session status + transcript. |
| `GET /sessions/{id}/report` | Evaluation report (available once status = `completed`). |
| *(internal)* Eval harness CLI/script | Republish historical session ids for regression scoring. |

---

## 9. Success Criteria

This is a learning project, so success is measured against learning + evaluator quality
outcomes, not user growth:

1. Evaluator agreement with hand-labels on the 30-session harness reaches a level the
   developer judges reliable per rubric item (not just in aggregate) — see HRN-3.
2. At least one concrete instance is found and fixed where the evaluator over-credits a
   transcript that a human would clearly score lower (the "capacity estimation never
   mentioned but scored well" class of bug) — this is the evidence that the evidence-first
   schema (EVL-4) is doing real work, not just adding ceremony.
3. The developer can explain, unprompted, why each infra component (Kafka, Redis, batched
   evaluation, prompt versioning) is present, including where it's overkill for the actual
   scale (§2.3), and defend that honestly under follow-up questioning.
4. End-to-end: a candidate can complete a full interview (all phases) and receive a
   structured, evidence-backed report within the target turnaround (EVL-8).

---

## 10. Risks & Open Questions

| Risk | Mitigation |
|---|---|
| Evaluator flattery/hallucinated evidence despite evidence-first schema. | This is the primary risk the eval harness (§HRN) exists to surface; treat any harness disagreement as a prompt bug, not noise. |
| Phase controller mis-detects phase completion on ambiguous answers ("it'll be a lot of traffic" as a capacity estimate). | Explicitly called out in the source plan as the hard/fuzzy case the LLM is there for; track these as harness cases too, not just rubric-scoring cases. |
| LLM API cost during iteration (harness re-runs against 30 sessions repeatedly). | Redis response cache keyed on prompt hash for dev-time replay (§Redis, infra table); track cost per session from day one (PER-3). |
| Over-engineering critique in an interview setting. | Preempted directly — see §2.3 and NFR-7; answer honestly that Kafka/Redis were chosen for learning/practice, not necessity. |
| Context window overflow on long sessions. | CTX-1/CTX-2 — summarize completed phases, never compress evaluator input. |
| Provider outages/rate limits stalling evaluation. | NFR-4 — Kafka consumer retry gives this "for free," per the source plan. |
| Free-tier evaluator underperforms on the harness (misses missing content, over-credits). | Documented, budgeted escalation path in §6.1: swap the evaluator only to a cheap paid model; interviewer/phase controller stay free regardless. |
| Free-tier rate limits (req/min, req/day) throttle usage during active development/testing. | NFR-9 + Redis prompt-hash cache to avoid re-spending calls on dev-time replays; if needed, stagger harness re-runs (§HRN-4) rather than firing all 30 sessions at once. |

---

## 11. Build Order / Milestones

Carried directly from the initial plan; each milestone should end with a working,
demoable slice, not partial work across multiple pieces.

- **Week 1** — Spring API, one hardcoded problem, single-shot LLM call (no streaming, no
  phases). Goal: prove the core loop end to end.
- **Week 2** — Transcript persistence, rubric table, evaluator (batched, evidence-first),
  and 10 hand-labeled sessions started.
- **Week 3** — Streaming over SSE, Angular UI, Redis session state, phase controller.
- **Week 4** — Kafka + async evaluation, cost tracking per session, prompt versioning.
- **Week 5+** — Iterate the evaluator prompt against the eval set; record agreement numbers
  per rubric item over time (this history is itself a portfolio artifact).

---

## 12. Tech Stack (as planned)

- **Backend:** Spring Boot (Java), `WebClient` calling provider REST APIs directly —
  no LangChain, no heavyweight multi-provider SDK abstraction.
- **LLM providers (cost-driven, see §6.1):**
  - **Google Gemini API (AI Studio)** — free tier, primary choice for interviewer and
    evaluator.
  - **Groq** — free tier, fast open-model inference, primary choice for phase controller
    and a viable alternative for the interviewer.
  - **Paid fallback (evaluator only, if needed):** a cheap tier model (Claude Haiku /
    GPT-4o-mini class) — not part of the default v1 setup, only invoked if free-tier
    evaluator accuracy proves insufficient on the harness (§HRN-3).
- **Frontend:** Angular.
- **Datastore:** PostgreSQL (durable), Redis (live state, response cache to protect free-tier
  rate limits, self rate-limiting).
- **Async/messaging:** Kafka.
- **Prompts:** Plain files under `src/main/resources/prompts/`, versioned in the filename,
  tracked in git so prompt history is real history.

---

## 13. Glossary

- **Phase** — A stage of the interview (requirements, estimation, high-level design, deep
  dive) with its own checklist of items the candidate should cover.
- **Rubric item** — A single gradeable criterion for a problem (e.g. "addressed hot key
  problem"), with a weight and reference notes describing a strong answer.
- **Evidence-first scoring** — Requiring a verbatim transcript quote before a verdict is
  produced, so the model cannot assert a status it cannot ground in the actual conversation.
- **Prompt version** — An identified, git-tracked revision of a prompt file, recorded against
  every turn/evaluation it produced, enabling before/after comparison.
