# Agent Domains

This map keeps domain ownership explicit so agent prompts and contributor docs do not drift apart.

## Domains

| Domain | Primary Agent | Main Files |
|---|---|---|
| xwOBA validation | `xwoba-validator` | `tests/test_xwoba_validation.py`, `src/processors/metric_calculator.py` |
| Daily MLB pipeline runs | `daily-pipeline-orchestrator` | `scripts/run_daily.py`, `src/collectors/`, `data/processed/` |
| MiLB prospects snapshots | `prospects-daily-runner` | `scripts/run_prospects_daily.py`, `src/collectors/prospects_collector.py`, `src/processors/prospect_normalizer.py` |
| Rolling-window validation | `window-aggregation-validator` | `src/processors/window_aggregator.py`, `tests/test_window_aggregator.py`, `tests/test_randomized_season_to_date.py` |
| Join coverage and enrichment quality | `data-quality-inspector` | `scripts/join_datasets.py`, `src/processors/data_joiner.py` |

## Maintenance Rule

If a new recurring workflow is added, either:

1. extend one of the documented domains above, or
2. add a new agent definition in `.github/agents/` and document the new domain here.

Do not duplicate agent prompt bodies in this directory.