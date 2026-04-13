import polars as pl


def aggregate_batter_game_stats(
    raw: pl.LazyFrame,
    pull_threshold: int,
) -> pl.LazyFrame:
    """Aggregate pitch-level Statcast data to one row per batter per game_date.

    Returns Statcast-derived metrics only: PA, BBE, xwOBA components, and
    pull air events. BB, K, and SB are sourced separately from the MLB Stats
    API and merged downstream in run_daily.py.
    """
    return (
        _aggregate_batting(raw, pull_threshold)
        .with_columns(pl.col("game_date").dt.year().cast(pl.Int16).alias("season"))
        .select([
            "game_date",
            "mlbam_id",
            "season",
            "pa",
            "bbe",
            "xwoba_num",
            "xwoba_denom",
            "pull_air_events",
        ])
    )


def _aggregate_batting(raw: pl.LazyFrame, pull_threshold: int) -> pl.LazyFrame:
    """Branch A: standard batting counts grouped by batter + game_date."""
    is_pull = (
        ((pl.col("stand") == "R") & (pl.col("hc_x") < pull_threshold))
        | ((pl.col("stand") == "L") & (pl.col("hc_x") > pull_threshold))
    )

    return (
        raw
        .group_by(["batter", "game_date"])
        .agg([
            (pl.col("woba_denom") == 1).sum().cast(pl.Int32).alias("pa"),
            (pl.col("type") == "X").sum().cast(pl.Int32).alias("bbe"),
            pl.when(pl.col("woba_denom") == 1)
            .then(pl.col("estimated_woba_using_speedangle"))
            .otherwise(0.0)
            .sum()
            .cast(pl.Float32)
            .alias("xwoba_num"),
            pl.col("woba_denom").sum().cast(pl.Int32).alias("xwoba_denom"),
            (is_pull & (pl.col("bb_type") == "fly_ball"))
            .sum()
            .cast(pl.Int32)
            .alias("pull_air_events"),
        ])
        .rename({"batter": "mlbam_id"})
        .cast({"mlbam_id": pl.Int32})
    )



