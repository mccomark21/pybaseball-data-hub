"""Unit tests for metric_calculator.aggregate_batter_game_stats.

Validates that xwoba_num correctly uses coalesce(estimated_woba_using_speedangle,
woba_value) for plate appearances, matching Baseball Savant's xwOBA methodology.
"""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from src.processors.metric_calculator import aggregate_batter_game_stats

PULL_THRESHOLD = 125


def _make_pitch_rows(rows: list[dict]) -> pl.LazyFrame:
    """Build a LazyFrame of pitch-level data from partial dicts.

    Fills in sensible defaults for columns not provided.
    """
    defaults = {
        "game_date": "2026-04-10",
        "batter": 123456,
        "stand": "R",
        "events": None,
        "type": "S",          # strike by default
        "bb_type": None,
        "estimated_woba_using_speedangle": None,
        "woba_value": None,
        "woba_denom": 0,
        "hc_x": None,
    }
    filled = [{**defaults, **r} for r in rows]
    return (
        pl.DataFrame(filled)
        .with_columns(pl.col("game_date").str.to_date())
        .lazy()
    )


class TestXwobaNumCalculation:
    """Core tests: xwoba_num must incorporate woba_value for non-batted-ball PAs."""

    def test_walk_contributes_to_xwoba_num(self):
        """A walk (woba_denom=1, no exit velo) should use woba_value ≈ 0.689."""
        lf = _make_pitch_rows([
            # Two non-PA pitches (balls) leading to the walk
            {"type": "B", "woba_denom": 0},
            {"type": "B", "woba_denom": 0},
            # The walk event itself
            {
                "type": "B",
                "events": "walk",
                "woba_denom": 1,
                "woba_value": 0.689,
                "estimated_woba_using_speedangle": None,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result["pa"][0] == 1
        assert result["xwoba_denom"][0] == 1
        assert result["xwoba_num"][0] == pytest.approx(0.689, abs=1e-3)

    def test_hbp_contributes_to_xwoba_num(self):
        """A hit-by-pitch should use woba_value ≈ 0.720."""
        lf = _make_pitch_rows([
            {
                "type": "B",
                "events": "hit_by_pitch",
                "woba_denom": 1,
                "woba_value": 0.720,
                "estimated_woba_using_speedangle": None,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result["xwoba_num"][0] == pytest.approx(0.720, abs=1e-3)

    def test_strikeout_contributes_zero(self):
        """A strikeout (woba_denom=1, woba_value=0) should add 0 to numerator."""
        lf = _make_pitch_rows([
            {
                "type": "S",
                "events": "strikeout",
                "woba_denom": 1,
                "woba_value": 0.0,
                "estimated_woba_using_speedangle": None,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result["xwoba_num"][0] == pytest.approx(0.0, abs=1e-6)
        assert result["xwoba_denom"][0] == 1

    def test_batted_ball_uses_estimated_woba(self):
        """A batted ball should prefer estimated_woba_using_speedangle over woba_value."""
        lf = _make_pitch_rows([
            {
                "type": "X",
                "events": "single",
                "bb_type": "line_drive",
                "woba_denom": 1,
                "woba_value": 0.870,  # actual outcome
                "estimated_woba_using_speedangle": 0.650,  # expected value
                "hc_x": 150,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        # Should use estimated (0.650), NOT actual (0.870)
        assert result["xwoba_num"][0] == pytest.approx(0.650, abs=1e-3)

    def test_non_pa_pitches_excluded_from_numerator(self):
        """Pitches with woba_denom=0 must not contribute to xwoba_num."""
        lf = _make_pitch_rows([
            # Two non-PA pitches
            {"type": "B", "woba_denom": 0, "woba_value": None},
            {"type": "S", "woba_denom": 0, "woba_value": None},
            # One actual PA (walk)
            {
                "type": "B",
                "events": "walk",
                "woba_denom": 1,
                "woba_value": 0.689,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result["pa"][0] == 1
        assert result["xwoba_denom"][0] == 1
        assert result["xwoba_num"][0] == pytest.approx(0.689, abs=1e-3)


class TestMixedGame:
    """Test a realistic game with multiple event types for a single batter."""

    def test_mixed_events_xwoba(self):
        """A game with BB, K, single, and fly-out should produce correct xwoba_num."""
        lf = _make_pitch_rows([
            # PA 1: Walk (3 balls + walk event)
            {"type": "B", "woba_denom": 0},
            {"type": "B", "woba_denom": 0},
            {"type": "B", "woba_denom": 0},
            {
                "type": "B",
                "events": "walk",
                "woba_denom": 1,
                "woba_value": 0.689,
                "estimated_woba_using_speedangle": None,
            },
            # PA 2: Strikeout
            {"type": "S", "woba_denom": 0},
            {"type": "S", "woba_denom": 0},
            {
                "type": "S",
                "events": "strikeout",
                "woba_denom": 1,
                "woba_value": 0.0,
                "estimated_woba_using_speedangle": None,
            },
            # PA 3: Single (batted ball)
            {"type": "S", "woba_denom": 0},
            {
                "type": "X",
                "events": "single",
                "bb_type": "line_drive",
                "woba_denom": 1,
                "woba_value": 0.870,
                "estimated_woba_using_speedangle": 0.750,
                "hc_x": 150,
            },
            # PA 4: Fly-out (batted ball)
            {
                "type": "X",
                "events": "field_out",
                "bb_type": "fly_ball",
                "woba_denom": 1,
                "woba_value": 0.0,
                "estimated_woba_using_speedangle": 0.050,
                "hc_x": 100,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result["pa"][0] == 4
        assert result["bbe"][0] == 2
        assert result["xwoba_denom"][0] == 4

        # xwoba_num = 0.689 (BB) + 0.0 (K) + 0.750 (single xwOBA) + 0.050 (flyout xwOBA)
        expected_num = 0.689 + 0.0 + 0.750 + 0.050
        assert result["xwoba_num"][0] == pytest.approx(expected_num, abs=1e-3)

        # xwOBA = xwoba_num / xwoba_denom
        expected_xwoba = expected_num / 4
        actual_xwoba = result["xwoba_num"][0] / result["xwoba_denom"][0]
        assert actual_xwoba == pytest.approx(expected_xwoba, abs=1e-3)


class TestEdgeCases:
    """Edge cases: single event type batters."""

    def test_all_walks_batter(self):
        """A batter with only walks should have xwOBA equal to walk weight."""
        walk_weight = 0.689
        lf = _make_pitch_rows([
            {
                "type": "B",
                "events": "walk",
                "woba_denom": 1,
                "woba_value": walk_weight,
            },
            {
                "type": "B",
                "events": "walk",
                "woba_denom": 1,
                "woba_value": walk_weight,
            },
            {
                "type": "B",
                "events": "walk",
                "woba_denom": 1,
                "woba_value": walk_weight,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result["pa"][0] == 3
        assert result["xwoba_denom"][0] == 3
        xwoba = result["xwoba_num"][0] / result["xwoba_denom"][0]
        assert xwoba == pytest.approx(walk_weight, abs=1e-3)

    def test_all_strikeouts_batter(self):
        """A batter with only strikeouts should have xwOBA of 0."""
        lf = _make_pitch_rows([
            {
                "type": "S",
                "events": "strikeout",
                "woba_denom": 1,
                "woba_value": 0.0,
            },
            {
                "type": "S",
                "events": "strikeout",
                "woba_denom": 1,
                "woba_value": 0.0,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result["pa"][0] == 2
        xwoba = result["xwoba_num"][0] / result["xwoba_denom"][0]
        assert xwoba == pytest.approx(0.0, abs=1e-6)

    def test_batted_balls_only_batter(self):
        """A batter with only batted balls should use estimated_woba exclusively."""
        lf = _make_pitch_rows([
            {
                "type": "X",
                "events": "home_run",
                "bb_type": "fly_ball",
                "woba_denom": 1,
                "woba_value": 2.007,
                "estimated_woba_using_speedangle": 1.950,
                "hc_x": 100,
            },
            {
                "type": "X",
                "events": "field_out",
                "bb_type": "ground_ball",
                "woba_denom": 1,
                "woba_value": 0.0,
                "estimated_woba_using_speedangle": 0.020,
                "hc_x": 150,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        expected_num = 1.950 + 0.020
        assert result["xwoba_num"][0] == pytest.approx(expected_num, abs=1e-3)

    def test_multiple_batters_separate_rows(self):
        """Two batters on the same date should produce two output rows."""
        lf = _make_pitch_rows([
            {
                "batter": 111,
                "type": "X",
                "events": "single",
                "bb_type": "line_drive",
                "woba_denom": 1,
                "woba_value": 0.870,
                "estimated_woba_using_speedangle": 0.800,
                "hc_x": 150,
            },
            {
                "batter": 222,
                "type": "B",
                "events": "walk",
                "woba_denom": 1,
                "woba_value": 0.689,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result.height == 2
        batter_111 = result.filter(pl.col("mlbam_id") == 111)
        batter_222 = result.filter(pl.col("mlbam_id") == 222)
        assert batter_111["xwoba_num"][0] == pytest.approx(0.800, abs=1e-3)
        assert batter_222["xwoba_num"][0] == pytest.approx(0.689, abs=1e-3)

    def test_output_schema(self):
        """Verify the output has the expected columns and types."""
        lf = _make_pitch_rows([
            {
                "type": "X",
                "events": "single",
                "bb_type": "line_drive",
                "woba_denom": 1,
                "woba_value": 0.870,
                "estimated_woba_using_speedangle": 0.800,
                "hc_x": 150,
            },
        ])
        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        expected_cols = {
            "game_date", "mlbam_id", "season", "pa", "bbe",
            "xwoba_num", "xwoba_denom", "pull_air_events",
        }
        assert set(result.columns) == expected_cols
        assert result["xwoba_num"].dtype == pl.Float32
        assert result["xwoba_denom"].dtype == pl.Int32


class TestPullAirEvents:
    """Unit tests for pull-side air-ball event counting."""

    def test_pull_threshold_boundary(self):
        """Boundary rule should exclude hc_x == threshold for both handedness values."""
        lf = _make_pitch_rows([
            {
                "stand": "R",
                "type": "X",
                "bb_type": "fly_ball",
                "woba_denom": 1,
                "hc_x": 124,
            },
            {
                "stand": "R",
                "type": "X",
                "bb_type": "fly_ball",
                "woba_denom": 1,
                "hc_x": 125,
            },
            {
                "stand": "L",
                "type": "X",
                "bb_type": "fly_ball",
                "woba_denom": 1,
                "hc_x": 125,
            },
            {
                "stand": "L",
                "type": "X",
                "bb_type": "fly_ball",
                "woba_denom": 1,
                "hc_x": 126,
            },
        ])

        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        # Pull events are R<125 and L>125, so only the 124 and 126 rows count.
        assert result["pull_air_events"][0] == 2
        assert result["bbe"][0] == 4

    def test_non_fly_balls_do_not_count(self):
        """Only fly balls should contribute to pull_air_events, even if pulled."""
        lf = _make_pitch_rows([
            {
                "stand": "R",
                "type": "X",
                "bb_type": "line_drive",
                "woba_denom": 1,
                "hc_x": 100,
            },
            {
                "stand": "L",
                "type": "X",
                "bb_type": "ground_ball",
                "woba_denom": 1,
                "hc_x": 150,
            },
            {
                "stand": "R",
                "type": "X",
                "bb_type": "fly_ball",
                "woba_denom": 1,
                "hc_x": 100,
            },
        ])

        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result["pull_air_events"][0] == 1
        assert result["bbe"][0] == 3

    def test_null_hc_x_is_excluded(self):
        """Rows missing hc_x should not be classified as pull-side air balls."""
        lf = _make_pitch_rows([
            {
                "stand": "R",
                "type": "X",
                "bb_type": "fly_ball",
                "woba_denom": 1,
                "hc_x": None,
            },
            {
                "stand": "R",
                "type": "X",
                "bb_type": "fly_ball",
                "woba_denom": 1,
                "hc_x": 90,
            },
        ])

        result = aggregate_batter_game_stats(lf, PULL_THRESHOLD).collect()

        assert result["pull_air_events"][0] == 1
        assert result["bbe"][0] == 2
