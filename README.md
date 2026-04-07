# pybaseball-data-hub

Daily collection of batter-level Statcast metrics for the current MLB season. A GitHub Actions cron job fetches full-season pitch data each night, aggregates it in memory via Polars, and commits two parquet files back to this repo. No external database required.

---

## How It Works

Each daily run:
1. Fetches all pitch-level Statcast data from Opening Day through yesterday via [pybaseball](https://github.com/jldbc/pybaseball)
2. Converts to Polars immediately and drops ~55 unneeded columns
3. Aggregates to one row per batter per game date (counting PA, BBE, xwOBA numerator/denominator, pull air events, BB, K, SB)
4. Overwrites `data/processed/batter_game_log.parquet` — full refresh handles retroactive Statcast corrections
5. Appends any newly-seen players to `data/processed/player_index.parquet`
6. Joins the game log with the player index and writes the enriched dataset to `data/processed/batter_game_log_enriched.parquet`

Stolen bases are attributed to the **actual runner**, not the batter at the plate, using Statcast's `on_1b`/`on_2b`/`on_3b` fields.

Only raw counts are stored. Rates (xwOBA, pull air %, BB:K, SB/PA) are computed at query time so they aggregate correctly across any date window.

---

## Output Files

| File | Rows (est. season end) | Size |
|---|---|---|
| `data/processed/batter_game_log.parquet` | ~144,000 (800 players × 180 dates) | < 1 MB |
| `data/processed/player_index.parquet` | ~800 | < 10 KB |
| `data/processed/batter_game_log_enriched.parquet` | ~144,000 | < 1 MB |

### `batter_game_log.parquet` schema

| Column | Type | Description |
|---|---|---|
| `game_date` | Date | |
| `mlbam_id` | Int32 | MLB Advanced Media batter ID |
| `season` | Int16 | Season year |
| `pa` | Int32 | Plate appearances |
| `bbe` | Int32 | Batted ball events |
| `xwoba_num` | Float32 | Sum of `estimated_woba_using_speedangle` |
| `xwoba_denom` | Int32 | Sum of `woba_denom` |
| `pull_air_events` | Int32 | Pull-side fly balls |
| `bb` | Int32 | Walks |
| `k` | Int32 | Strikeouts |
| `sb` | Int32 | Stolen bases (attributed to runner) |

### `player_index.parquet` schema

| Column | Type | Description |
|---|---|---|
| `player_name` | Utf8 | "Last, First" format |
| `mlbam_id` | Int32 | Join key to game log |
| `key_bbref` | Utf8 | Baseball Reference ID |
| `key_fangraphs` | Int32 | FanGraphs ID |

### `batter_game_log_enriched.parquet` schema

Produced by joining `batter_game_log.parquet` with `player_index.parquet` on `mlbam_id`. Contains all game-log columns plus the player-identity columns from the index.

| Column | Type | Description |
|---|---|---|
| `player_name` | Utf8 | "Last, First" format |
| `mlbam_id` | Int32 | MLB Advanced Media batter ID |
| `key_bbref` | Utf8 | Baseball Reference ID |
| `key_fangraphs` | Int32 | FanGraphs ID |
| `game_date` | Date | |
| `season` | Int16 | Season year |
| `pa` | Int32 | Plate appearances |
| `bbe` | Int32 | Batted ball events |
| `xwoba_num` | Float32 | Sum of `estimated_woba_using_speedangle` |
| `xwoba_denom` | Int32 | Sum of `woba_denom` |
| `pull_air_events` | Int32 | Pull-side fly balls |
| `bb` | Int32 | Walks |
| `k` | Int32 | Strikeouts |
| `sb` | Int32 | Stolen bases (attributed to runner) |

### Sample data — 3 players (one game each)

| player_name | mlbam_id | key_bbref | key_fangraphs | game_date | season | pa | bbe | xwoba_num | xwoba_denom | pull_air_events | bb | k | sb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| abrams, cj | 682928 | abramcj01 | 25768 | 2026-03-26 | 2026 | 4 | 3 | 1.974 | 4 | 0 | 0 | 1 | 0 |
| abreu, wilyer | 677800 | abreuwi02 | 23772 | 2026-03-26 | 2026 | 3 | 3 | 0.478 | 3 | 0 | 0 | 1 | 0 |
| acuña, luisangel | 682668 | acunajo01 | -1 | 2026-03-26 | 2026 | 3 | 3 | 0.811 | 3 | 0 | 0 | 0 | 0 |

---

## Running Locally

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts/run_daily.py
```

To test with a shorter window, edit `season_start` in `config/metrics.yaml` temporarily.

### Querying the data

```python
import polars as pl
from datetime import date, timedelta

lf = pl.scan_parquet("data/processed/batter_game_log.parquet")
index = pl.read_parquet("data/processed/player_index.parquet")

cutoff = date.today() - timedelta(days=30)

stats = (
    lf
    .filter(pl.col("game_date") >= cutoff)
    .group_by("mlbam_id")
    .agg([
        pl.sum("pa"),
        pl.sum("bbe"),
        (pl.sum("xwoba_num") / pl.sum("xwoba_denom")).alias("xwoba"),
        (pl.sum("pull_air_events") / pl.sum("bbe")).alias("pull_air_pct"),
        (pl.sum("bb") / pl.sum("k")).alias("bbk_ratio"),
        (pl.sum("sb") / pl.sum("pa")).alias("sb_per_pa"),
    ])
    .collect()
    .join(index, on="mlbam_id", how="left")
)
```

Available windows: 7 / 14 / 30 / 60 / 90 days, or season-to-date (omit the filter).

---

## Project Structure

```
data/processed/
  batter_game_log.parquet         # Fully rewritten daily
  player_index.parquet            # Append-only player ID map
  batter_game_log_enriched.parquet # Game log joined with player index (written daily)
src/
  collectors/
    statcast_collector.py         # pybaseball fetch → Polars conversion boundary
    player_index_builder.py       # Resolves new MLBAM IDs via reverse lookup
  processors/
    metric_calculator.py          # Two-branch aggregation (batting + SB attribution)
    window_aggregator.py          # Date-windowed rate aggregation
    data_joiner.py                # Joins game log with player index
scripts/
  run_daily.py                    # GitHub Actions entry point (collection + aggregation)
  join_datasets.py                # Joins processed files into enriched dataset
.github/workflows/
  daily_collect.yml               # Cron schedule (5:00 UTC daily) + git commit/push
config/
  metrics.yaml                    # KEEP_COLS, pull threshold, SB event map, season start
```

---

## Configuration

All tunable values live in `config/metrics.yaml`:

- **`season_start`** — update each year to the MLB Opening Day date
- **`pull_threshold`** — `hc_x` boundary (default `125`) for pull-side direction
- **`keep_cols`** — Statcast columns retained after fetch (all others dropped immediately)
- **`sb_event_map`** — maps SB event types to their runner columns

---

## Dependencies

| Package | Purpose |
|---|---|
| `pybaseball>=2.2.6` | Statcast data fetching |
| `polars` | All data processing and aggregation |
| `pyarrow` | Parquet I/O backend for Polars |
| `pyyaml` | Config loading |
