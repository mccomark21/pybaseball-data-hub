import datetime
from pathlib import Path

import polars as pl
import pytest

from src.processors.daily_game_log_module import (
    DailyGameLogConfig,
    DailyGameLogModule,
    DailyGameLogRequest,
)


class FakeStatcastAdapter:
    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame
        self.calls: list[tuple[str, str]] = []

    def fetch(self, start_date: str, end_date: str, keep_cols: list[str]) -> pl.DataFrame:
        self.calls.append((start_date, end_date))
        return self._frame


class FakeMlbBoxscoreAdapter:
    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame
        self.calls: list[tuple[str, str]] = []

    def fetch(self, start_date: str, end_date: str) -> pl.DataFrame:
        self.calls.append((start_date, end_date))
        return self._frame


def _raw_pitch_rows(game_date: datetime.date, batter: int = 123) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_date": [game_date],
            "batter": [batter],
            "stand": ["R"],
            "events": ["single"],
            "type": ["X"],
            "bb_type": ["fly_ball"],
            "estimated_woba_using_speedangle": [0.5],
            "woba_value": [0.9],
            "woba_denom": [1],
            "hc_x": [140],
        }
    )


def _config(tmp_path: Path, season_start: str = "2026-03-26") -> DailyGameLogConfig:
    return DailyGameLogConfig(
        season_start=season_start,
        keep_cols=[
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
        ],
        pull_threshold=125,
        game_log_path=tmp_path / "batter_game_log.parquet",
    )


def test_no_data_short_circuits_without_boxscore_call(tmp_path: Path):
    statcast = FakeStatcastAdapter(pl.DataFrame(schema={"game_date": pl.Date}))
    boxscore = FakeMlbBoxscoreAdapter(
        pl.DataFrame(
            {
                "game_date": [datetime.date(2026, 4, 9)],
                "mlbam_id": [123],
                "bb": [1],
                "k": [1],
                "sb": [0],
            }
        )
    )
    module = DailyGameLogModule(_config(tmp_path), statcast_adapter=statcast, boxscore_adapter=boxscore)

    result = module.refresh(DailyGameLogRequest(mode="daily", as_of=datetime.date(2026, 4, 10)))

    assert result.status == "no_data"
    assert result.batch_rows == 0
    assert result.total_rows == 0
    assert boxscore.calls == []
    assert not (tmp_path / "batter_game_log.parquet").exists()


def test_daily_mode_appends_and_dedups_with_new_rows_winning(tmp_path: Path):
    path = tmp_path / "batter_game_log.parquet"
    existing = pl.DataFrame(
        {
            "game_date": [datetime.date(2026, 4, 9), datetime.date(2026, 4, 8)],
            "mlbam_id": [123, 999],
            "season": [2026, 2026],
            "pa": [1, 3],
            "bbe": [1, 2],
            "xwoba_num": [0.1, 0.6],
            "xwoba_denom": [1, 3],
            "pull_air_events": [0, 1],
            "bb": [0, 1],
            "k": [1, 1],
            "sb": [0, 0],
        }
    )
    existing.write_parquet(str(path))

    statcast = FakeStatcastAdapter(_raw_pitch_rows(datetime.date(2026, 4, 9), batter=123))
    boxscore = FakeMlbBoxscoreAdapter(
        pl.DataFrame(
            {
                "game_date": [datetime.date(2026, 4, 9)],
                "mlbam_id": [123],
                "bb": [2],
                "k": [0],
                "sb": [1],
            }
        )
    )
    module = DailyGameLogModule(_config(tmp_path), statcast_adapter=statcast, boxscore_adapter=boxscore)

    result = module.refresh(DailyGameLogRequest(mode="daily", as_of=datetime.date(2026, 4, 10)))

    assert result.status == "written"
    assert result.batch_rows == 1
    assert result.total_rows == 2
    written = pl.read_parquet(str(path)).sort(["mlbam_id", "game_date"])
    updated = written.filter((pl.col("mlbam_id") == 123) & (pl.col("game_date") == datetime.date(2026, 4, 9)))
    assert updated["bb"][0] == 2
    assert updated["sb"][0] == 1


def test_full_season_mode_overwrites_existing_file(tmp_path: Path):
    path = tmp_path / "batter_game_log.parquet"
    pl.DataFrame(
        {
            "game_date": [datetime.date(2026, 4, 1)],
            "mlbam_id": [1],
            "season": [2026],
            "pa": [1],
            "bbe": [1],
            "xwoba_num": [0.1],
            "xwoba_denom": [1],
            "pull_air_events": [0],
            "bb": [0],
            "k": [1],
            "sb": [0],
        }
    ).write_parquet(str(path))

    statcast = FakeStatcastAdapter(_raw_pitch_rows(datetime.date(2026, 4, 9), batter=123))
    boxscore = FakeMlbBoxscoreAdapter(
        pl.DataFrame(
            {
                "game_date": [datetime.date(2026, 4, 9)],
                "mlbam_id": [123],
                "bb": [0],
                "k": [1],
                "sb": [0],
            }
        )
    )
    module = DailyGameLogModule(_config(tmp_path), statcast_adapter=statcast, boxscore_adapter=boxscore)

    result = module.refresh(DailyGameLogRequest(mode="full_season", as_of=datetime.date(2026, 4, 10)))

    assert result.status == "written"
    assert result.total_rows == 1
    written = pl.read_parquet(str(path))
    assert written.height == 1
    assert written["mlbam_id"][0] == 123


def test_missing_boxscore_values_are_zero_filled(tmp_path: Path):
    statcast = FakeStatcastAdapter(_raw_pitch_rows(datetime.date(2026, 4, 9), batter=123))
    boxscore = FakeMlbBoxscoreAdapter(
        pl.DataFrame(
            schema={
                "game_date": pl.Date,
                "mlbam_id": pl.Int32,
                "bb": pl.Int32,
                "k": pl.Int32,
                "sb": pl.Int32,
            }
        )
    )
    module = DailyGameLogModule(_config(tmp_path), statcast_adapter=statcast, boxscore_adapter=boxscore)

    result = module.refresh(DailyGameLogRequest(mode="daily", as_of=datetime.date(2026, 4, 10)))

    assert result.status == "written"
    written = pl.read_parquet(str(tmp_path / "batter_game_log.parquet"))
    assert written["bb"][0] == 0
    assert written["k"][0] == 0
    assert written["sb"][0] == 0
