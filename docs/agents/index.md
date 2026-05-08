# Agents

This directory documents how the repository's custom agents are intended to be used.

Executable definitions live in `.github/agents/`. These docs describe when to use each agent, which repository surface it owns, and where follow-up work should be routed.

## Agent List

| Agent | Primary Use | Main Surfaces |
|---|---|---|
| `xwoba-validator` | Validate xwOBA behavior against tests and tolerance rules | `tests/test_xwoba_validation.py`, `src/processors/metric_calculator.py` |
| `daily-pipeline-orchestrator` | Run and verify daily or full-season MLB pipeline refreshes | `scripts/run_daily.py`, `data/processed/` |
| `prospects-daily-runner` | Build and validate MiLB prospects snapshots | `scripts/run_prospects_daily.py`, `config/prospects_config.yaml` |
| `window-aggregation-validator` | Check rolling-window and season-to-date aggregation consistency | `src/processors/window_aggregator.py`, `tests/test_window_aggregator.py` |
| `data-quality-inspector` | Inspect join coverage and unmatched MLBAM IDs | `scripts/join_datasets.py`, `src/processors/data_joiner.py` |

## Usage Rules

- Keep prompt logic in `.github/agents/*.agent.md`.
- Keep contributor-facing process notes in this directory.
- Prefer focused validation commands over broad test suites.
- Open or link a GitHub Issue when the change affects routing, operational policy, or repeated triage decisions.

## Related Docs

- `triage.md` for issue labels and intake rules
- `domains.md` for domain-to-agent mapping
- `../adr/0001-deepen-daily-game-log-workflow.md` for the current daily game-log module boundary decision