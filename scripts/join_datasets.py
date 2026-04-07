"""Join processed parquet files into an enriched batter dataset."""

import sys
from pathlib import Path

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.processors.data_joiner import join_datasets

GAME_LOG_PATH = PROJECT_ROOT / "data" / "processed" / "batter_game_log.parquet"
PLAYER_INDEX_PATH = PROJECT_ROOT / "data" / "processed" / "player_index.parquet"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "batter_game_log_enriched.parquet"


def main() -> None:
    print(f"Game log:     {GAME_LOG_PATH}")
    print(f"Player index: {PLAYER_INDEX_PATH}")
    print(f"Output:       {OUTPUT_PATH}")
    print()

    report = join_datasets(
        game_log_path=str(GAME_LOG_PATH),
        player_index_path=str(PLAYER_INDEX_PATH),
        output_path=str(OUTPUT_PATH),
    )

    report.print_summary()


if __name__ == "__main__":
    main()
