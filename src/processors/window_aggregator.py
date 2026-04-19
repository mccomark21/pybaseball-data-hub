import datetime

import polars as pl


def get_windowed_stats(
    window_days: int | None,
    game_log_path: str,
    index_path: str,
) -> pl.DataFrame:
    """Aggregate batter stats over a rolling window (or season-to-date if None).

    Reads the game log lazily, filters to the window, computes rates/totals,
    and joins player names from the index.
    """
    lf = pl.scan_parquet(game_log_path)

    if window_days is not None:
        cutoff = datetime.date.today() - datetime.timedelta(days=window_days)
        lf = lf.filter(pl.col("game_date") >= cutoff)

    agg = (
        lf
        .group_by("mlbam_id")
        .agg([
            pl.sum("pa"),
            pl.sum("bbe"),
            (pl.sum("xwoba_num") / pl.sum("xwoba_denom")).alias("xwoba"),
            (pl.sum("pull_air_events") / pl.sum("bbe")).alias("pull_air_pct"),
            (pl.sum("bb") / pl.sum("k")).alias("bbk_ratio"),
            pl.sum("sb").alias("sb_total"),
        ])
        .collect()
    )

    index = pl.read_parquet(index_path)
    return agg.join(index, on="mlbam_id", how="left")
