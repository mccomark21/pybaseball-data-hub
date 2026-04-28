"""Tests for MiLB stats enrichment collection."""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.collectors import milb_stats_collector as collector


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400 and self.status_code != 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses

    def get(self, *args, **kwargs) -> _FakeResponse:
        return self._responses.pop(0)


def test_resolve_mlbam_id_returns_first_match() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                {
                    "people": [
                        {"id": 111111, "fullName": "Player A"},
                        {"id": 222222, "fullName": "Player B"},
                    ]
                }
            )
        ]
    )

    result = collector.resolve_mlbam_id("Player A", session=session)
    assert result == 111111


def test_fetch_milb_group_pool_stats_collects_prefixed_fields() -> None:
    session = _FakeSession(
        [
            _FakeResponse(
                {
                    "stats": [
                        {
                            "splits": [
                                {
                                    "player": {"id": 123},
                                    "sport": {"id": 11},
                                    "stat": {
                                        "plateAppearances": 20,
                                        "hits": 6,
                                        "doubles": 1,
                                        "ops": ".830",
                                    },
                                },
                                {
                                    "player": {"id": 456},
                                    "sport": {"id": 14},
                                    "stat": {
                                        "plateAppearances": 5,
                                        "hits": 2,
                                        "ops": ".700",
                                    },
                                },
                            ]
                        }
                    ]
                }
            )
        ]
    )

    result = collector.fetch_milb_group_pool_stats(
        group="hitting",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 28),
        session=session,
    )

    assert set(result.keys()) == {123, 456}
    assert result[123]["mlbam_id"] == 123
    assert result[123]["plateAppearances"] == 20
    assert result[123]["hits"] == 6
    assert result[123]["doubles"] == 1
    assert result[123]["ops"] == ".830"


def test_collect_prospect_window_stats_emits_rows_for_all_windows(monkeypatch) -> None:
    source_rows = pd.DataFrame(
        [
            {"player_name": "Player A", "org": "Org A", "level": "AA"},
        ]
    )

    monkeypatch.setattr(collector, "resolve_mlbam_id", lambda **kwargs: 444444)

    def _fake_fetch_group_pool(**kwargs):
        if kwargs["group"] == "hitting":
            return {
                444444: {
                    "mlbam_id": 444444,
                    "plateAppearances": 12,
                    "hits": 4,
                    "ops": ".760",
                }
            }
        return {
            444444: {
                "mlbam_id": 444444,
                "inningsPitched": "9.0",
                "strikeOuts": 11,
                "baseOnBalls": 3,
                "hits": 8,
            }
        }

    monkeypatch.setattr(collector, "fetch_milb_group_pool_stats", _fake_fetch_group_pool)

    result = collector.collect_prospect_window_stats(
        source_rows=source_rows,
        windows=(("STD", None), ("7D", 7)),
        as_of_date=date(2026, 4, 28),
    )

    assert result.shape[0] == 2
    assert set(result["window"].tolist()) == {"STD", "7D"}
    assert result["mlbam_id"].tolist() == [444444, 444444]
    assert result["plateAppearances"].tolist() == [12, 12]
    assert result["strikeOuts"].tolist() == [11, 11]
    assert result["hits"].tolist() == [4, 4]
    assert result["hits_pitching"].tolist() == [8, 8]
