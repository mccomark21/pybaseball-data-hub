"""Tests for MiLB prospect source-row normalization and window expansion."""

from __future__ import annotations

import pandas as pd

from src.processors.prospect_normalizer import build_prospects_snapshot


def test_snapshot_expands_to_four_windows_per_player() -> None:
    source_rows = pd.DataFrame(
        [
            {
                "source": "mlb",
                "rank": 1,
                "player_name": "Player A",
                "org": "Org A",
                "level": "AA",
                "positions": "SS",
                "age": 21,
                "eta": "2026",
                "bats": "R",
                "throws": "R",
                "fv": "60",
                "ofp": 62,
                "stats_summary": "summary",
                "scouting_report": "report",
                "notes": "notes",
                "payload_scraped_at": "2026-04-28T00:00:00Z",
                "collected_at": "2026-04-28T01:00:00+00:00",
                "source_url": "https://example.test/prospects.json",
            },
            {
                "source": "fangraphs",
                "rank": 3,
                "player_name": "Player A",
                "org": "Org A",
                "level": "AA",
                "positions": "SS",
                "age": 21,
                "eta": "2026",
                "bats": "R",
                "throws": "R",
                "fv": "60",
                "ofp": 62,
                "stats_summary": "summary",
                "scouting_report": "report",
                "notes": "notes",
                "payload_scraped_at": "2026-04-28T00:00:00Z",
                "collected_at": "2026-04-28T01:00:00+00:00",
                "source_url": "https://example.test/prospects.json",
            },
        ]
    )

    snapshot = build_prospects_snapshot(source_rows)

    assert snapshot.shape[0] == 4
    assert set(snapshot["window"].tolist()) == {"STD", "30D", "14D", "7D"}


def test_snapshot_rolls_up_source_ranks() -> None:
    source_rows = pd.DataFrame(
        [
            {
                "source": "mlb",
                "rank": 2,
                "player_name": "Player X",
                "org": "Org X",
                "level": "AAA",
                "positions": "1B",
                "age": 22,
                "eta": "2026",
                "bats": "L",
                "throws": "R",
                "fv": "55",
                "ofp": 57,
                "stats_summary": None,
                "scouting_report": None,
                "notes": None,
                "payload_scraped_at": "2026-04-28T00:00:00Z",
                "collected_at": "2026-04-28T01:00:00+00:00",
                "source_url": "https://example.test/prospects.json",
            },
            {
                "source": "prospects_live",
                "rank": 6,
                "player_name": "Player X",
                "org": "Org X",
                "level": "AAA",
                "positions": "1B",
                "age": 22,
                "eta": "2026",
                "bats": "L",
                "throws": "R",
                "fv": "55",
                "ofp": 57,
                "stats_summary": None,
                "scouting_report": None,
                "notes": None,
                "payload_scraped_at": "2026-04-28T00:00:00Z",
                "collected_at": "2026-04-28T01:00:00+00:00",
                "source_url": "https://example.test/prospects.json",
            },
        ]
    )

    snapshot = build_prospects_snapshot(source_rows)
    std_row = snapshot[snapshot["window"] == "STD"].iloc[0]

    assert std_row["mlb_rank"] == 2
    assert pd.isna(std_row["fangraphs_rank"])
    assert std_row["prospects_live_rank"] == 6
    assert std_row["best_rank"] == 2
    assert std_row["avg_rank"] == 4


def test_snapshot_merges_window_stats_when_provided() -> None:
    source_rows = pd.DataFrame(
        [
            {
                "source": "mlb",
                "rank": 2,
                "player_name": "Player X",
                "org": "Org X",
                "level": "AAA",
                "positions": "1B",
                "age": 22,
                "eta": "2026",
                "bats": "L",
                "throws": "R",
                "fv": "55",
                "ofp": 57,
                "stats_summary": None,
                "scouting_report": None,
                "notes": None,
                "payload_scraped_at": "2026-04-28T00:00:00Z",
                "collected_at": "2026-04-28T01:00:00+00:00",
                "source_url": "https://example.test/prospects.json",
            }
        ]
    )

    stats_rows = pd.DataFrame(
        [
            {
                "player_name": "Player X",
                "org": "Org X",
                "level": "AAA",
                "window": "7D",
                "mlbam_id": 123456,
                "plateAppearances": 22,
                "hits": 10,
                "ops": ".820",
                "inningsPitched": "4.0",
                "strikeOuts": 7,
            }
        ]
    )

    snapshot = build_prospects_snapshot(source_rows, stats_rows=stats_rows)

    row_7d = snapshot[snapshot["window"] == "7D"].iloc[0]
    row_std = snapshot[snapshot["window"] == "STD"].iloc[0]

    assert row_7d["mlbam_id"] == 123456
    assert row_7d["plateAppearances"] == 22
    assert row_7d["hits"] == 10
    assert row_7d["ops"] == ".820"
    assert row_7d["inningsPitched"] == "4.0"
    assert row_7d["strikeOuts"] == 7
    assert pd.isna(row_std["plateAppearances"])
