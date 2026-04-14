"""Daily Statcast collection entry point for GitHub Actions."""

import argparse
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
from src.collectors.mlb_api_collector import fetch_boxscore_batting_stats
from src.processors.metric_calculator import aggregate_batter_game_stats

CONFIG_PATH = PROJECT_ROOT / "config" / "metrics.yaml"
GAME_LOG_PATH = PROJECT_ROOT / "data" / "processed" / "batter_game_log.parquet"
PLAYER_INDEX_PATH = PROJECT_ROOT / "data" / "processed" / "player_index.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and process Statcast data.")
    parser.add_argument(
        "--full-season",
        action="store_true",
        help="Reprocess the full season from season_start in config. "
             "Overwrites the existing game log instead of appending.",
    )
    return parser.parse_args()


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def main() -> None:
    args = parse_args()
    config = load_config()

    et = ZoneInfo("America/New_York")
    today = datetime.now(et)
    yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    if args.full_season:
        start_date = config["season_start"]
        print(f"Fetching FULL SEASON Statcast data: {start_date} to {yesterday}")
    else:
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

    statcast_agg = aggregate_batter_game_stats(
        raw=raw.lazy(),
        pull_threshold=config["pull_threshold"],
    ).collect()

    print(f"Aggregated to {statcast_agg.height:,} new batter-game rows.")
    print(f"Fetching MLB API batting stats: {start_date} to {yesterday}...")

    mlb_stats = fetch_boxscore_batting_stats(
        start_date=start_date,
        end_date=yesterday,
    )

    game_log = (
        statcast_agg.lazy()
        .join(mlb_stats.lazy(), on=["mlbam_id", "game_date"], how="left")
        .with_columns([
            pl.col("bb").fill_null(0),
            pl.col("k").fill_null(0),
            pl.col("sb").fill_null(0),
        ])
        .select([
            "game_date",
            "mlbam_id",
            "season",
            "pa",
            "bbe",
            "xwoba_num",
            "xwoba_denom",
            "pull_air_events",
            "bb",
            "k",
            "sb",
        ])
        .collect()
    )

    print(f"Merged {game_log.height:,} batter-game rows.")

    GAME_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not args.full_season and GAME_LOG_PATH.exists():
        existing = pl.read_parquet(str(GAME_LOG_PATH))
        game_log = (
            pl.concat([game_log, existing])
            .unique(subset=["mlbam_id", "game_date"], keep="first")
        )
    game_log.write_parquet(str(GAME_LOG_PATH))
    mode = "overwrote" if args.full_season else "wrote"
    print(f"{mode.capitalize()} {GAME_LOG_PATH.name} ({game_log.height:,} total batter-game rows)")

    mlbam_ids = game_log["mlbam_id"].unique().to_list()
    update_player_index(mlbam_ids, str(PLAYER_INDEX_PATH))
    print(f"Updated {PLAYER_INDEX_PATH}")


if __name__ == "__main__":
    main()
