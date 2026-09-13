# AI System Design Interviewer

A practice tool for system design interviews: an LLM plays the interviewer through a
structured, multi-phase conversation, then a separate evaluation pass scores the transcript
against a per-problem rubric — with evidence quotes required before any verdict, to avoid the
generic "great job, 7/10" failure mode common to LLM-graded feedback.

This is a learning project. Its explicit goal is hands-on, defensible experience integrating
multiple LLM calls (different roles, different models) into one system, and building a real
evaluation harness for LLM output quality — see the docs below for the full reasoning,
including where the architecture is deliberately more than a single-user tool strictly needs.

## Docs

- [Requirements analysis](docs/requirements-analysis.md) — what the system does, why, and the
  explicit tradeoffs (including cost policy: free-tier LLM APIs by default).
- [Implementation plan](docs/implementation-plan.md) — project structure, schema, API
  contracts, and a week-by-week build order.

## Status

Early build-out, following the Week 1–5+ plan in the implementation doc.
