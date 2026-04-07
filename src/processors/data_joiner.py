"""Join batter_game_log with player_index into an enriched dataset."""

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl


@dataclass
class JoinReport:
    """Summary produced by :func:`join_datasets`."""

    total_game_log_rows: int
    total_player_index_rows: int
    output_rows: int
    matched_unique_players: int
    unmatched_mlbam_ids: list[int] = field(default_factory=list)

    def print_summary(self) -> None:
        """Print a human-readable summary to stdout."""
        print("=" * 50)
        print("Join Summary")
        print("=" * 50)
        print(f"  Game-log rows (after dedup):    {self.total_game_log_rows:>6,}")
        print(f"  Player-index rows (after dedup):{self.total_player_index_rows:>6,}")
        print(f"  Output rows:                    {self.output_rows:>6,}")
        print(f"  Unique players matched:         {self.matched_unique_players:>6,}")
        unmatched_count = len(self.unmatched_mlbam_ids)
        print(f"  Unmatched MLBAM IDs:            {unmatched_count:>6,}")
        if self.unmatched_mlbam_ids:
            print()
            print("  The following MLBAM IDs appear in the game log but could")
            print("  not be found in the player index:")
            for mlbam_id in self.unmatched_mlbam_ids:
                print(f"    - {mlbam_id}")
        print("=" * 50)


def join_datasets(
    game_log_path: str,
    player_index_path: str,
    output_path: str,
) -> JoinReport:
    """Join batter_game_log with player_index on ``mlbam_id``.

    Steps
    -----
    1. Read both parquet files.
    2. Deduplicate each source to avoid inflating row counts.
    3. Left-join the game log with the player index on ``mlbam_id`` so that
       every game-log row is retained even when no player-index match exists.
    4. Deduplicate the result again as a safety measure.
    5. Write the enriched dataset to *output_path*.
    6. Return a :class:`JoinReport` describing the outcome, including any
       MLBAM IDs that could not be matched.

    Parameters
    ----------
    game_log_path:
        String path to ``batter_game_log.parquet``.
    player_index_path:
        String path to ``player_index.parquet``.
    output_path:
        String destination path for the enriched parquet file.

    Returns
    -------
    JoinReport
    """
    game_log = pl.read_parquet(game_log_path).unique()
    player_index = pl.read_parquet(player_index_path).unique(subset=["mlbam_id"])

    game_log_ids: set[int] = set(game_log["mlbam_id"].to_list())
    index_ids: set[int] = set(player_index["mlbam_id"].to_list())
    unmatched_ids = sorted(game_log_ids - index_ids)

    joined = (
        game_log
        .join(player_index, on="mlbam_id", how="left")
        .unique()
        .select([
            "player_name",
            "mlbam_id",
            "key_bbref",
            "key_fangraphs",
            "game_date",
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
    )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    joined.write_parquet(output_path)

    matched_unique_players = (
        joined
        .filter(pl.col("player_name").is_not_null())
        .select(pl.col("mlbam_id").n_unique())
        .item()
    )

    return JoinReport(
        total_game_log_rows=game_log.height,
        total_player_index_rows=player_index.height,
        output_rows=joined.height,
        matched_unique_players=matched_unique_players,
        unmatched_mlbam_ids=unmatched_ids,
    )
