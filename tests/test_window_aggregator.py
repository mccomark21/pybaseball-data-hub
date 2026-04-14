"""Tests for window_aggregator.get_windowed_stats.

Validates that season-to-date and windowed aggregation produce correct xwOBA
from game-level parquet data.
"""

import datetime
import tempfile
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from src.processors.window_aggregator import get_windowed_stats


@pytest.fixture()
def sample_data(tmp_path: Path) -> tuple[str, str]:
    """Create sample game-log and index parquet files, return their paths."""
    game_log = pl.DataFrame({
        "game_date": [
            datetime.date(2026, 4, 1),
            datetime.date(2026, 4, 2),
            datetime.date(2026, 4, 5),
            datetime.date(2026, 4, 8),
            # Second batter — just one game
            datetime.date(2026, 4, 3),
        ],
        "mlbam_id": [100, 100, 100, 100, 200],
        "season": [2026, 2026, 2026, 2026, 2026],
        "pa": [4, 3, 5, 4, 3],
        "bbe": [2, 1, 3, 2, 1],
        # xwoba_num includes woba_value contributions for BB/HBP/K
        "xwoba_num": [1.5, 0.8, 2.0, 1.2, 0.9],
        "xwoba_denom": [4, 3, 5, 4, 3],
        "pull_air_events": [1, 0, 2, 1, 0],
        "bb": [1, 1, 0, 1, 1],
        "k": [1, 0, 2, 1, 1],
        "sb": [0, 1, 0, 0, 0],
    })

    index = pl.DataFrame({
        "mlbam_id": [100, 200],
        "player_name": ["Test Player A", "Test Player B"],
        "key_bbref": ["playea01", "playeb01"],
        "key_fangraphs": [12345, 67890],
    })

    gl_path = str(tmp_path / "game_log.parquet")
    idx_path = str(tmp_path / "index.parquet")
    game_log.write_parquet(gl_path)
    index.write_parquet(idx_path)

    return gl_path, idx_path


class TestSeasonToDate:
    """window_days=None should aggregate the entire dataset."""

    def test_xwoba_season_to_date(self, sample_data):
        gl_path, idx_path = sample_data
        result = get_windowed_stats(None, gl_path, idx_path)

        player_a = result.filter(pl.col("mlbam_id") == 100)
        assert player_a.height == 1

        # Sum of xwoba_num for batter 100: 1.5 + 0.8 + 2.0 + 1.2 = 5.5
        # Sum of xwoba_denom: 4 + 3 + 5 + 4 = 16
        expected_xwoba = 5.5 / 16
        assert player_a["xwoba"][0] == pytest.approx(expected_xwoba, abs=1e-4)

    def test_pa_sum(self, sample_data):
        gl_path, idx_path = sample_data
        result = get_windowed_stats(None, gl_path, idx_path)

        player_a = result.filter(pl.col("mlbam_id") == 100)
        assert player_a["pa"][0] == 16  # 4 + 3 + 5 + 4

        player_b = result.filter(pl.col("mlbam_id") == 200)
        assert player_b["pa"][0] == 3

    def test_player_names_joined(self, sample_data):
        gl_path, idx_path = sample_data
        result = get_windowed_stats(None, gl_path, idx_path)

        player_a = result.filter(pl.col("mlbam_id") == 100)
        assert player_a["player_name"][0] == "Test Player A"


class TestWindowedAggregation:
    """window_days != None should filter to the last N days."""

    def test_7_day_window(self, sample_data):
        """7-day window from April 10 should include Apr 4-10, so Apr 5 and Apr 8."""
        gl_path, idx_path = sample_data

        mock_date = datetime.date(2026, 4, 10)
        with patch("src.processors.window_aggregator.datetime") as mock_dt:
            mock_dt.date.today.return_value = mock_date
            mock_dt.timedelta = datetime.timedelta
            result = get_windowed_stats(7, gl_path, idx_path)

        player_a = result.filter(pl.col("mlbam_id") == 100)
        # Cutoff = Apr 10 - 7 = Apr 3. Games on/after Apr 3: Apr 5 and Apr 8
        # xwoba_num: 2.0 + 1.2 = 3.2, xwoba_denom: 5 + 4 = 9
        expected_xwoba = 3.2 / 9
        assert player_a["xwoba"][0] == pytest.approx(expected_xwoba, abs=1e-4)
        assert player_a["pa"][0] == 9

    def test_3_day_window(self, sample_data):
        """3-day window from April 10 should include Apr 8-10, so only Apr 8."""
        gl_path, idx_path = sample_data

        mock_date = datetime.date(2026, 4, 10)
        with patch("src.processors.window_aggregator.datetime") as mock_dt:
            mock_dt.date.today.return_value = mock_date
            mock_dt.timedelta = datetime.timedelta
            result = get_windowed_stats(3, gl_path, idx_path)

        player_a = result.filter(pl.col("mlbam_id") == 100)
        # Cutoff = Apr 10 - 3 = Apr 7. Only game on/after Apr 7: Apr 8
        expected_xwoba = 1.2 / 4
        assert player_a["xwoba"][0] == pytest.approx(expected_xwoba, abs=1e-4)
        assert player_a["pa"][0] == 4

    def test_window_excludes_filtered_players(self, sample_data):
        """Batter 200 only has a game on Apr 3; a 3-day window from Apr 10 should exclude them."""
        gl_path, idx_path = sample_data

        mock_date = datetime.date(2026, 4, 10)
        with patch("src.processors.window_aggregator.datetime") as mock_dt:
            mock_dt.date.today.return_value = mock_date
            mock_dt.timedelta = datetime.timedelta
            result = get_windowed_stats(3, gl_path, idx_path)

        player_b = result.filter(pl.col("mlbam_id") == 200)
        assert player_b.height == 0


class TestEdgeCases:
    """Edge cases for the aggregator."""

    def test_empty_game_log(self, tmp_path):
        """Empty game log should produce empty result."""
        gl = pl.DataFrame({
            "game_date": pl.Series([], dtype=pl.Date),
            "mlbam_id": pl.Series([], dtype=pl.Int32),
            "season": pl.Series([], dtype=pl.Int16),
            "pa": pl.Series([], dtype=pl.Int32),
            "bbe": pl.Series([], dtype=pl.Int32),
            "xwoba_num": pl.Series([], dtype=pl.Float32),
            "xwoba_denom": pl.Series([], dtype=pl.Int32),
            "pull_air_events": pl.Series([], dtype=pl.Int32),
            "bb": pl.Series([], dtype=pl.Int32),
            "k": pl.Series([], dtype=pl.Int32),
            "sb": pl.Series([], dtype=pl.Int32),
        })
        idx = pl.DataFrame({
            "mlbam_id": pl.Series([], dtype=pl.Int32),
            "player_name": pl.Series([], dtype=pl.Utf8),
            "key_bbref": pl.Series([], dtype=pl.Utf8),
            "key_fangraphs": pl.Series([], dtype=pl.Int32),
        })

        gl_path = str(tmp_path / "empty_gl.parquet")
        idx_path = str(tmp_path / "empty_idx.parquet")
        gl.write_parquet(gl_path)
        idx.write_parquet(idx_path)

        result = get_windowed_stats(None, gl_path, idx_path)
        assert result.height == 0
