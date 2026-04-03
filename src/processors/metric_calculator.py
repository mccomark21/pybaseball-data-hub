import polars as pl


def aggregate_batter_game_stats(
    raw: pl.LazyFrame,
    pull_threshold: int,
    sb_event_map: dict[str, str],
) -> pl.LazyFrame:
    """Aggregate pitch-level Statcast data to one row per batter per game_date.

    Two branches are computed and joined:
      A) Batting stats grouped by (batter, game_date)
      B) Stolen bases attributed to the actual runner via on-base columns
    """
    batting = _aggregate_batting(raw, pull_threshold)
    stolen_bases = _aggregate_stolen_bases(raw, sb_event_map)

    result = (
        batting
        .join(stolen_bases, on=["mlbam_id", "game_date"], how="left")
        .with_columns(pl.col("sb").fill_null(0))
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
            "bb",
            "k",
            "sb",
        ])
    )
    return result


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
            (pl.col("events") == "walk").sum().cast(pl.Int32).alias("bb"),
            pl.col("events")
            .is_in(["strikeout", "strikeout_double_play"])
            .sum()
            .cast(pl.Int32)
            .alias("k"),
        ])
        .rename({"batter": "mlbam_id"})
        .cast({"mlbam_id": pl.Int32})
    )


def _aggregate_stolen_bases(
    raw: pl.LazyFrame, sb_event_map: dict[str, str]
) -> pl.LazyFrame:
    """Branch B: attribute each stolen base to the actual runner.

    sb_event_map maps event names to the on-base column identifying the runner:
      stolen_base_2b  -> on_1b   (runner who was on 1st stole 2nd)
      stolen_base_3b  -> on_2b   (runner who was on 2nd stole 3rd)
      stolen_base_home -> on_3b  (runner who was on 3rd stole home)
    """
    sb_events = list(sb_event_map.keys())
    runner_cols = list(sb_event_map.values())

    sb_rows = (
        raw
        .filter(pl.col("events").is_in(sb_events))
        .select([
            "game_date",
            "events",
            *runner_cols,
        ])
    )

    # Build a runner_id column by matching the event to its runner column
    runner_expr = pl.lit(None, dtype=pl.Float64)
    for event, col in sb_event_map.items():
        runner_expr = (
            pl.when(pl.col("events") == event)
            .then(pl.col(col))
            .otherwise(runner_expr)
        )

    return (
        sb_rows
        .with_columns(runner_expr.cast(pl.Int32).alias("mlbam_id"))
        .filter(pl.col("mlbam_id").is_not_null())
        .group_by(["mlbam_id", "game_date"])
        .agg(pl.len().cast(pl.Int32).alias("sb"))
    )
