import argparse
from datetime import date
from pathlib import Path

import polars as pl

import scripts.run_daily as run_daily
from src.processors.daily_game_log_module import DailyGameLogResult


def test_main_prints_no_data_and_exits_early(monkeypatch, capsys):
    monkeypatch.setattr(run_daily, "parse_args", lambda: argparse.Namespace(full_season=False))
    monkeypatch.setattr(
        run_daily,
        "load_config",
        lambda: {
            "season_start": "2026-03-26",
            "keep_cols": ["game_date", "batter"],
            "pull_threshold": 125,
        },
    )

    class FakeModule:
        def __init__(self, *_args, **_kwargs):
            pass

        def refresh(self, _request):
            return DailyGameLogResult(
                status="no_data",
                mode="daily",
                start_date=date(2026, 4, 7),
                end_date=date(2026, 4, 9),
                batch_rows=0,
                total_rows=0,
                output_path=Path("unused.parquet"),
            )

    monkeypatch.setattr(run_daily, "DailyGameLogModule", FakeModule)
    monkeypatch.setattr(
        run_daily.pl,
        "read_parquet",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("read_parquet should not be called")),
    )
    monkeypatch.setattr(
        run_daily,
        "update_player_index",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("update_player_index should not be called")),
    )

    run_daily.main()
    out = capsys.readouterr().out
    assert "No data returned. Exiting." in out


def test_main_full_season_reports_and_updates_index(monkeypatch, capsys):
    monkeypatch.setattr(run_daily, "parse_args", lambda: argparse.Namespace(full_season=True))
    monkeypatch.setattr(
        run_daily,
        "load_config",
        lambda: {
            "season_start": "2026-03-26",
            "keep_cols": ["game_date", "batter"],
            "pull_threshold": 125,
        },
    )

    class FakeModule:
        def __init__(self, *_args, **_kwargs):
            pass

        def refresh(self, request):
            assert request.mode == "full_season"
            return DailyGameLogResult(
                status="written",
                mode="full_season",
                start_date=date(2026, 3, 26),
                end_date=date(2026, 4, 9),
                batch_rows=5,
                total_rows=123,
                output_path=Path("fake_output.parquet"),
            )

    monkeypatch.setattr(run_daily, "DailyGameLogModule", FakeModule)
    monkeypatch.setattr(
        run_daily.pl,
        "read_parquet",
        lambda *_args, **_kwargs: pl.DataFrame({"mlbam_id": [100, 200, 100]}),
    )

    calls = []

    def _record_update(ids, index_path):
        calls.append((ids, index_path))

    monkeypatch.setattr(run_daily, "update_player_index", _record_update)

    run_daily.main()
    out = capsys.readouterr().out

    assert "Overwrote fake_output.parquet (123 total batter-game rows) for 2026-03-26 to 2026-04-09" in out
    assert "Updated" in out
    assert len(calls) == 1
    assert set(calls[0][0]) == {100, 200}
    assert calls[0][1] == str(run_daily.PLAYER_INDEX_PATH)
