# Agent Triage

## Tracker

Use GitHub Issues as the canonical place to track agent changes, regressions, and follow-up work.

## Baseline Labels

Use one baseline status label:

- `bug`
- `enhancement`
- `new feature`

Add one domain label when the issue is tied to a specific analysis area or pipeline slice.

## Intake Guidance

Create or update an issue when:

- an agent's deterministic workflow changes
- a recurring validation failure needs follow-up outside the current patch
- the repository needs a new domain agent or domain boundary changes

Keep issue titles explicit about the workflow surface, for example:

- `bug: xwoba validator drift exceeds tolerance`
- `enhancement: daily pipeline orchestrator verify enriched parquet row counts`
- `new feature: add agent guidance for prospects normalization checks`