"""Daily Statcast collection entry point for GitHub Actions."""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import yaml

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.collectors.player_index_builder import update_player_index
from src.collectors.statcast_collector import fetch_season_statcast
from src.processors.metric_calculator import aggregate_batter_game_stats

CONFIG_PATH = PROJECT_ROOT / "config" / "metrics.yaml"
GAME_LOG_PATH = PROJECT_ROOT / "data" / "processed" / "batter_game_log.parquet"
PLAYER_INDEX_PATH = PROJECT_ROOT / "data" / "processed" / "player_index.parquet"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def main() -> None:
    config = load_config()

    et = ZoneInfo("America/New_York")
    today = datetime.now(et)
    yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")

    print(f"Fetching Statcast data: {start_date} to {yesterday} (3-day window)")

    raw = fetch_season_statcast(
        start_date=start_date,
        end_date=yesterday,
        keep_cols=config["keep_cols"],
    )

    if raw.is_empty():
        print("No data returned. Exiting.")
        return

    print(f"Fetched {raw.height:,} pitch rows. Aggregating...")

    game_log = aggregate_batter_game_stats(
        raw=raw.lazy(),
        pull_threshold=config["pull_threshold"],
        sb_event_map=config["sb_event_map"],
    ).collect()

    print(f"Aggregated to {game_log.height:,} new batter-game rows.")

    GAME_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if GAME_LOG_PATH.exists():
        existing = pl.read_parquet(str(GAME_LOG_PATH))
        game_log = (
            pl.concat([game_log, existing])
            .unique(subset=["mlbam_id", "game_date"], keep="first")
        )
    game_log.write_parquet(str(GAME_LOG_PATH))
    print(f"Wrote {GAME_LOG_PATH.name} ({game_log.height:,} total batter-game rows)")

    mlbam_ids = game_log["mlbam_id"].unique().to_list()
    update_player_index(mlbam_ids, str(PLAYER_INDEX_PATH))
    print(f"Updated {PLAYER_INDEX_PATH}")


if __name__ == "__main__":
    main()
