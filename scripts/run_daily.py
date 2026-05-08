"""Daily Statcast collection entry point for GitHub Actions."""

import argparse
import sys
from pathlib import Path

import polars as pl
import yaml

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.collectors.player_index_builder import update_player_index
from src.processors.daily_game_log_module import (
    DailyGameLogConfig,
    DailyGameLogModule,
    DailyGameLogRequest,
)

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

    mode = "full_season" if args.full_season else "daily"
    request = DailyGameLogRequest(mode=mode)
    module = DailyGameLogModule(
        DailyGameLogConfig(
            season_start=config["season_start"],
            keep_cols=config["keep_cols"],
            pull_threshold=config["pull_threshold"],
            game_log_path=GAME_LOG_PATH,
        )
    )

    result = module.refresh(request)

    if result.status == "no_data":
        print("No data returned. Exiting.")
        return

    verb = "Overwrote" if args.full_season else "Wrote"
    print(
        f"{verb} {result.output_path.name} ({result.total_rows:,} total batter-game rows) "
        f"for {result.start_date.isoformat()} to {result.end_date.isoformat()}"
    )

    game_log = pl.read_parquet(str(result.output_path))
    mlbam_ids = game_log["mlbam_id"].unique().to_list()
    update_player_index(mlbam_ids, str(PLAYER_INDEX_PATH))
    print(f"Updated {PLAYER_INDEX_PATH}")


if __name__ == "__main__":
    main()
