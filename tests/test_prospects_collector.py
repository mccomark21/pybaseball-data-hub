"""Tests for MiLB prospects payload parsing and strict level filtering."""

from __future__ import annotations

from datetime import datetime, timezone

from src.collectors.prospects_collector import parse_prospects_payload


def test_parse_payload_filters_to_explicit_milb_levels_only() -> None:
    payload = {
        "scraped_at": "2026-04-28T00:00:00Z",
        "sources": [{"source": "mlb", "ok": True}, {"source": "fangraphs", "ok": True}],
        "rows": [
            {
                "source": "mlb",
                "rank": 1,
                "player_name": "Player A",
                "org": "Org A",
                "level": "AA",
                "positions": ["SS"],
                "age": 21,
            },
            {
                "source": "fangraphs",
                "rank": 5,
                "player_name": "Player A",
                "org": "ORG",
                "level": "MLB",
                "positions": ["OF"],
                "age": 24,
            },
            {
                "source": "prospects_live",
                "rank": 7,
                "player_name": "Player C",
                "org": "Org C",
                "level": "A+",
                "positions": ["3B", "SS"],
                "age": 20,
            },
        ],
    }

    df = parse_prospects_payload(
        payload=payload,
        allowed_levels=["A", "A+", "AA", "AAA"],
        collected_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
    )

    assert df.shape[0] == 1
    assert df["player_name"].tolist() == ["Player C"]
    assert df["level"].tolist() == ["A+"]


def test_parse_payload_preserves_core_fields() -> None:
    payload = {
        "scraped_at": "2026-04-28T00:00:00Z",
        "sources": [{"source": "mlb", "ok": True}],
        "rows": [
            {
                "source": "mlb",
                "rank": "2",
                "player_name": "Player Z",
                "org": "Org Z",
                "level": "A",
                "positions": ["C", "1B"],
                "age": "19",
                "eta": "2027",
                "bats": "L",
                "throws": "R",
                "fv": "55",
                "ofp": "58",
                "stats_summary": "Sample summary",
            }
        ],
    }

    df = parse_prospects_payload(payload=payload)
    row = df.iloc[0]

    assert row["player_name"] == "Player Z"
    assert row["positions"] == "C|1B"
    assert row["rank"] == 2
    assert row["age"] == 19
    assert row["ofp"] == 58
    assert row["payload_scraped_at"] == "2026-04-28T00:00:00Z"


def test_parse_payload_applies_fangraphs_profile_fields_before_filtering() -> None:
    payload = {
        "scraped_at": "2026-04-28T00:00:00Z",
        "sources": [
            {"source": "mlb", "ok": True},
            {"source": "fangraphs", "ok": True},
        ],
        "rows": [
            {
                "source": "mlb",
                "rank": 2,
                "player_name": "Player X",
                "org": "Org X",
                "level": "AA",
                "positions": ["1B"],
                "age": 22,
                "bats": "L",
            },
            {
                "source": "fangraphs",
                "rank": 1,
                "player_name": "Player X",
                "org": "ORGX",
                "level": "AAA",
                "positions": ["3B"],
                "age": 21,
                "bats": "R",
            },
        ],
    }

    df = parse_prospects_payload(
        payload=payload,
        allowed_levels=["A", "A+", "AA", "AAA"],
        collected_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
    )

    assert df.shape[0] == 2
    assert set(df["source"].tolist()) == {"mlb", "fangraphs"}
    assert set(df["level"].tolist()) == {"AAA"}
    assert set(df["org"].tolist()) == {"ORGX"}
    assert set(df["positions"].tolist()) == {"3B"}
    assert set(df["age"].tolist()) == {21}
    assert set(df["bats"].tolist()) == {"R"}
