"""Integration tests: validate xwOBA against Baseball Savant.

Fetches real Statcast data for known players over a completed date range,
computes xwOBA using our formula, and compares against Savant's published
xwOBA values scraped from the leaderboard.

These tests require network access and are marked with @pytest.mark.integration.
Run with:  pytest tests/test_xwoba_validation.py -v -m integration
"""

import polars as pl
import pytest

from src.processors.metric_calculator import aggregate_batter_game_stats

# ---------------------------------------------------------------------------
# Tolerance: Savant rounds to 3 decimal places. Minor pitch-level data
# revisions can cause ~0.001-0.003 drift between our calculation and the
# published number.
# ---------------------------------------------------------------------------
XWOBA_TOLERANCE = 0.003

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PULL_THRESHOLD = 125

# Columns we need from Statcast (must match config/metrics.yaml keep_cols + woba_value)
KEEP_COLS = [
    "game_date",
    "batter",
    "stand",
    "events",
    "type",
    "bb_type",
    "estimated_woba_using_speedangle",
    "woba_value",
    "woba_denom",
    "hc_x",
]


def _fetch_and_compute_xwoba(
    mlbam_id: int,
    start_date: str,
    end_date: str,
) -> float:
    """Fetch raw Statcast for one player and compute xwOBA using our pipeline."""
    from pybaseball import cache, statcast_batter

    cache.enable()

    raw_pd = statcast_batter(start_dt=start_date, end_dt=end_date, player_id=mlbam_id)

    if raw_pd.empty:
        pytest.skip(f"No Statcast data for batter {mlbam_id} in [{start_date}, {end_date}]")

    raw = (
        pl.from_pandas(raw_pd)
        .select(KEEP_COLS)
        .with_columns(pl.col("game_date").cast(pl.Date))
    )

    agg = aggregate_batter_game_stats(
        raw.lazy(), PULL_THRESHOLD
    ).collect()

    total_num = agg["xwoba_num"].sum()
    total_denom = agg["xwoba_denom"].sum()

    if total_denom == 0:
        pytest.skip(f"No plate appearances for batter {mlbam_id}")

    return total_num / total_denom


def _fetch_savant_xwoba(mlbam_id: int, season: int) -> float:
    """Fetch a player's season xwOBA from Baseball Savant expected stats.

    Uses the Savant CSV endpoint for expected statistics.
    """
    import requests

    url = (
        "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
        f"?type=batter&year={season}&position=&team=&min=1"
        f"&csv=true"
    )
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    df = pl.read_csv(resp.content)

    # Column names vary slightly; find the xwOBA column
    xwoba_col = None
    for col in df.columns:
        if "xwoba" in col.lower() or "est_woba" in col.lower():
            xwoba_col = col
            break
    if xwoba_col is None:
        pytest.fail(f"Could not find xwOBA column in Savant CSV. Columns: {df.columns}")

    # Find player ID column
    id_col = None
    for col in df.columns:
        if "player_id" in col.lower() or "mlbam" in col.lower() or col == "player_id":
            id_col = col
            break
    if id_col is None:
        # Try 'batter' as fallback
        if "batter" in df.columns:
            id_col = "batter"
        else:
            pytest.fail(f"Could not find player ID column. Columns: {df.columns}")

    player_row = df.filter(pl.col(id_col).cast(pl.Int64) == mlbam_id)
    if player_row.is_empty():
        pytest.skip(f"Player {mlbam_id} not found in Savant leaderboard for {season}")

    return float(player_row[xwoba_col][0])


# ---------------------------------------------------------------------------
# Test cases — use 2025 completed season data for stable comparisons.
# These MLBAM IDs and expected values are for well-known players with enough
# PAs to produce stable xwOBA. The Savant lookup serves as ground truth.
# ---------------------------------------------------------------------------

# 2025 full season (completed) — stable data for validation
VALIDATION_SEASON = 2025
VALIDATION_START = "2025-03-27"
VALIDATION_END = "2025-09-28"

# Players chosen for diversity of batting profiles:
#   - Aaron Judge (592450): high-walk, power hitter
#   - Bobby Witt Jr (677951): contact/speed
#   - Juan Soto (665742): elite walk rate + power
VALIDATION_PLAYERS = [
    pytest.param(592450, id="aaron_judge"),
    pytest.param(677951, id="bobby_witt_jr"),
    pytest.param(665742, id="juan_soto"),
]


@pytest.mark.integration
@pytest.mark.parametrize("mlbam_id", VALIDATION_PLAYERS)
def test_xwoba_matches_savant(mlbam_id: int):
    """Computed xwOBA should be within tolerance of Baseball Savant's value."""
    our_xwoba = _fetch_and_compute_xwoba(
        mlbam_id, VALIDATION_START, VALIDATION_END
    )
    savant_xwoba = _fetch_savant_xwoba(mlbam_id, VALIDATION_SEASON)

    diff = abs(our_xwoba - savant_xwoba)
    print(
        f"\n  Player {mlbam_id}: "
        f"ours={our_xwoba:.4f}, savant={savant_xwoba:.4f}, diff={diff:.4f}"
    )
    assert diff <= XWOBA_TOLERANCE, (
        f"xwOBA mismatch for player {mlbam_id}: "
        f"ours={our_xwoba:.4f}, savant={savant_xwoba:.4f}, diff={diff:.4f} "
        f"(tolerance={XWOBA_TOLERANCE})"
    )


@pytest.mark.integration
def test_xwoba_formula_consistency():
    """Verify our per-game aggregation then season sum matches a direct season calc.

    This catches rounding/accumulation errors in the two-step process
    (pitch → game aggregation, then game → season aggregation).
    """
    from pybaseball import cache, statcast_batter

    cache.enable()

    mlbam_id = 592450  # Aaron Judge
    raw_pd = statcast_batter(
        start_dt=VALIDATION_START, end_dt=VALIDATION_END, player_id=mlbam_id
    )
    if raw_pd.empty:
        pytest.skip("No data")

    raw = (
        pl.from_pandas(raw_pd)
        .select(KEEP_COLS)
        .with_columns(pl.col("game_date").cast(pl.Date))
    )

    # Method 1: aggregate per-game, then sum across games (our pipeline)
    per_game = aggregate_batter_game_stats(raw.lazy(), PULL_THRESHOLD).collect()
    pipeline_xwoba = per_game["xwoba_num"].sum() / per_game["xwoba_denom"].sum()

    # Method 2: direct season-level calculation from pitch data
    pa_pitches = raw.filter(pl.col("woba_denom") == 1)
    direct_num = (
        pa_pitches
        .select(
            pl.coalesce(
                pl.col("estimated_woba_using_speedangle"),
                pl.col("woba_value"),
            )
            .fill_null(0.0)
        )
        .to_series()
        .sum()
    )
    direct_denom = pa_pitches.height
    direct_xwoba = direct_num / direct_denom

    diff = abs(pipeline_xwoba - direct_xwoba)
    print(
        f"\n  Pipeline xwOBA={pipeline_xwoba:.6f}, "
        f"Direct xwOBA={direct_xwoba:.6f}, diff={diff:.6f}"
    )
    # These should match very closely — any difference is purely Float32 rounding
    assert diff < 0.001, (
        f"Pipeline vs direct mismatch: {pipeline_xwoba:.6f} vs {direct_xwoba:.6f}"
    )
