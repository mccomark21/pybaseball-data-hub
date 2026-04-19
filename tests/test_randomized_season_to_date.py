"""Offline randomized season-to-date validation over high-volume players.

These tests never hit the network. They validate that window_aggregator season-to-date
results match direct recomputation from local game-log aggregates for a daily-varying,
deterministic sample of high-volume players.
"""

from __future__ import annotations

import datetime
import random
from pathlib import Path

import polars as pl
import pytest

from src.processors.window_aggregator import get_windowed_stats

REPO_ROOT = Path(__file__).resolve().parents[1]
GAME_LOG_PATH = REPO_ROOT / "data" / "processed" / "batter_game_log.parquet"
INDEX_PATH = REPO_ROOT / "data" / "processed" / "player_index.parquet"

# Pick thresholds high enough to focus on meaningful season-to-date volume while
# still keeping enough candidates in partial-season local snapshots.
MIN_PA = 80
MIN_BBE = 30
SAMPLE_SIZE = 20
EPS = 1e-7


def _date_seed() -> int:
    """Return a deterministic daily seed in YYYYMMDD integer form."""
    return int(datetime.date.today().strftime("%Y%m%d"))


def _sample_high_volume_players(game_log: pl.DataFrame, seed: int) -> list[int]:
    """Sample high-volume players deterministically for the given seed."""
    eligible = (
        game_log
        .group_by("mlbam_id")
        .agg([
            pl.sum("pa").alias("pa_total"),
            pl.sum("bbe").alias("bbe_total"),
        ])
        .filter(
            (pl.col("pa_total") >= MIN_PA)
            & (pl.col("bbe_total") >= MIN_BBE)
        )
        .select("mlbam_id")
        .to_series()
        .to_list()
    )

    if not eligible:
        return []

    rng = random.Random(seed)
    shuffled = list(sorted(eligible))
    rng.shuffle(shuffled)
    return shuffled[: min(SAMPLE_SIZE, len(shuffled))]


def _manual_season_to_date(game_log: pl.DataFrame) -> pl.DataFrame:
    """Recompute season-to-date metrics directly from stored per-game aggregates."""
    return game_log.group_by("mlbam_id").agg([
        pl.sum("pa").alias("pa"),
        pl.sum("bbe").alias("bbe"),
        (pl.sum("xwoba_num") / pl.sum("xwoba_denom")).alias("xwoba_manual"),
        (pl.sum("pull_air_events") / pl.sum("bbe")).alias("pull_air_pct_manual"),
        (pl.sum("bb") / pl.sum("k")).alias("bbk_ratio_manual"),
        pl.sum("sb").alias("sb_total_manual"),
    ])


@pytest.mark.offline
@pytest.mark.randomized
def test_season_to_date_randomized_high_volume_metrics_align():
    """Validate all in-scope season-to-date metrics for a daily-randomized sample."""
    if not GAME_LOG_PATH.exists() or not INDEX_PATH.exists():
        pytest.skip("Processed parquet files are not available in this workspace")

    game_log = pl.read_parquet(str(GAME_LOG_PATH))
    if game_log.is_empty():
        pytest.skip("Game log is empty")

    seed = _date_seed()
    sampled_ids = _sample_high_volume_players(game_log, seed)
    if not sampled_ids:
        pytest.skip(
            f"No eligible high-volume players found (MIN_PA={MIN_PA}, MIN_BBE={MIN_BBE})"
        )

    aggregated = get_windowed_stats(None, str(GAME_LOG_PATH), str(INDEX_PATH)).filter(
        pl.col("mlbam_id").is_in(sampled_ids)
    )
    manual = _manual_season_to_date(game_log).filter(pl.col("mlbam_id").is_in(sampled_ids))

    merged = aggregated.join(manual, on="mlbam_id", how="inner")

    if merged.height != len(sampled_ids):
        pytest.fail(
            "Missing sampled players in merged validation frame: "
            f"seed={seed}, sampled={sampled_ids}, merged_rows={merged.height}"
        )

    failures: list[str] = []
    for row in merged.iter_rows(named=True):
        mlbam_id = row["mlbam_id"]

        xwoba_diff = abs(float(row["xwoba"]) - float(row["xwoba_manual"]))
        pull_air_diff = abs(float(row["pull_air_pct"]) - float(row["pull_air_pct_manual"]))
        bbk_diff = abs(float(row["bbk_ratio"]) - float(row["bbk_ratio_manual"]))
        sb_match = int(row["sb_total"]) == int(row["sb_total_manual"])

        if xwoba_diff > EPS:
            failures.append(
                f"xwoba mlbam_id={mlbam_id} diff={xwoba_diff:.10f} seed={seed}"
            )
        if pull_air_diff > EPS:
            failures.append(
                f"pull_air_pct mlbam_id={mlbam_id} diff={pull_air_diff:.10f} seed={seed}"
            )
        if bbk_diff > EPS:
            failures.append(
                f"bbk_ratio mlbam_id={mlbam_id} diff={bbk_diff:.10f} seed={seed}"
            )
        if not sb_match:
            failures.append(
                "sb_total "
                f"mlbam_id={mlbam_id} actual={row['sb_total']} "
                f"expected={row['sb_total_manual']} seed={seed}"
            )

    assert not failures, (
        "Season-to-date randomized validation mismatches:\n"
        + "\n".join(failures)
        + f"\nseed={seed}\nsampled_ids={sampled_ids}"
    )


@pytest.mark.offline
def test_daily_seed_sampling_is_deterministic_for_same_seed():
    """Same seed should produce the exact same player sample ordering."""
    if not GAME_LOG_PATH.exists():
        pytest.skip("Game log is not available")

    game_log = pl.read_parquet(str(GAME_LOG_PATH))
    if game_log.is_empty():
        pytest.skip("Game log is empty")

    seed = _date_seed()
    sample_a = _sample_high_volume_players(game_log, seed)
    sample_b = _sample_high_volume_players(game_log, seed)
    assert sample_a == sample_b
