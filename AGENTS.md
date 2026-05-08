# AGENTS

This repository keeps executable agent definitions in `.github/agents/`.

Use this file as the contributor-facing index for agent usage, issue routing, and documentation locations. Keep agent prompt logic in `.github/agents/*.agent.md` and keep supporting prose in `docs/agents/`.

## Issue Tracker

- Canonical tracker: GitHub Issues
- Open or update an issue before changing agent behavior, routing, or validation workflow when the work is expected to span multiple commits.

## Triage Labels

Use the existing repository labels when possible. For agent-related work, route issues with one or more of these labels:

- `bug`
- `enhancement`
- `new feature`

Add a domain-specific label when the issue clearly belongs to one of the documented agent domains in `docs/agents/`.

## Source Of Truth

- Runtime agent definitions: `.github/agents/*.agent.md`
- Contributor documentation: `docs/agents/`
- High-level workspace summary: `README.md`

## Agent Catalog

- `xwoba-validator`
- `daily-pipeline-orchestrator`
- `prospects-daily-runner`
- `window-aggregation-validator`
- `data-quality-inspector`

## Documentation Layout

- `docs/agents/index.md`: agent catalog and ownership boundaries
- `docs/agents/triage.md`: issue routing and label guidance
- `docs/agents/domains.md`: domain map for recurring investigation areas

When updating an agent:

1. Edit the matching file in `.github/agents/`.
2. Update the relevant file in `docs/agents/` if the workflow, ownership boundary, or routing guidance changed.
3. Update `README.md` only when the public summary changed.