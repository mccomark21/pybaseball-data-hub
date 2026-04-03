from pathlib import Path

import polars as pl
from pybaseball import playerid_reverse_lookup


def update_player_index(current_mlbam_ids: list[int], index_path: str) -> None:
    """Append any newly-seen MLBAM IDs to the player index parquet file."""
    path = Path(index_path)
    schema = {
        "player_name": pl.Utf8,
        "mlbam_id": pl.Int32,
        "key_bbref": pl.Utf8,
        "key_fangraphs": pl.Int32,
    }

    if path.exists():
        existing = pl.read_parquet(index_path)
        known_ids = set(existing["mlbam_id"].to_list())
    else:
        existing = pl.DataFrame(schema=schema)
        known_ids = set()

    new_ids = sorted(set(current_mlbam_ids) - known_ids)
    if not new_ids:
        return

    lookup_pd = playerid_reverse_lookup(new_ids, key_type="key_mlbam")
    if lookup_pd.empty:
        return

    lookup = pl.from_pandas(lookup_pd)

    new_rows = (
        lookup
        .with_columns(
            (pl.col("name_last") + ", " + pl.col("name_first")).alias("player_name")
        )
        .select([
            "player_name",
            pl.col("key_mlbam").cast(pl.Int32).alias("mlbam_id"),
            pl.col("key_bbref").cast(pl.Utf8),
            pl.col("key_fangraphs").cast(pl.Int32),
        ])
    )

    updated = pl.concat([existing, new_rows])
    updated.write_parquet(index_path)
