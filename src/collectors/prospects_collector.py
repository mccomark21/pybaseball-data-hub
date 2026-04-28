"""Collector for deployed Yahoo app prospects API.

This module fetches and parses the prospects payload exposed by the deployed app,
then filters rows to explicit MiLB levels only.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import pandas as pd
import requests

DEFAULT_PROSPECTS_URL = (
    "https://mccomark21.github.io/yahoo-fantasy-baseball-eval-app/api/prospects/latest.json"
)
DEFAULT_ALLOWED_LEVELS = frozenset({"A", "A+", "AA", "AAA"})


def normalize_level(level: Any) -> str | None:
    """Normalize level labels to canonical short codes."""
    if level is None:
        return None

    text = str(level).strip()
    if not text:
        return None

    up = text.upper()
    if up in {"A", "A+", "AA", "AAA", "MLB", "ROK"}:
        return up
    if "TRIPLE" in up and "A" in up:
        return "AAA"
    if "DOUBLE" in up and "A" in up:
        return "AA"
    if "HIGH" in up and "A" in up:
        return "A+"
    if "LOW" in up and "A" in up:
        return "A"
    if "SINGLE" in up and "A" in up:
        return "A"
    return up


def fetch_prospects_payload(
    source_url: str = DEFAULT_PROSPECTS_URL,
    timeout: int = 30,
) -> dict[str, Any]:
    """Fetch the raw prospects payload from the deployed API endpoint."""
    response = requests.get(source_url, timeout=timeout)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Prospects payload must be a JSON object")
    if not isinstance(payload.get("rows"), list):
        raise ValueError("Prospects payload is missing rows")
    if not isinstance(payload.get("sources"), list):
        raise ValueError("Prospects payload is missing source statuses")

    return payload


def parse_prospects_payload(
    payload: Mapping[str, Any],
    source_url: str = DEFAULT_PROSPECTS_URL,
    allowed_levels: Iterable[str] = DEFAULT_ALLOWED_LEVELS,
    collected_at: datetime | None = None,
) -> pd.DataFrame:
    """Parse and strictly filter prospects rows to explicit MiLB levels only."""
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError("Prospects payload is missing rows")

    allowed_set = {str(v).upper() for v in allowed_levels}
    collected = collected_at or datetime.now(timezone.utc)

    parsed_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue

        normalized_level = normalize_level(row.get("level"))
        if normalized_level is None or normalized_level not in allowed_set:
            continue

        player_name = row.get("player_name")
        org = row.get("org")
        if not player_name or not org:
            continue

        positions = row.get("positions")
        if isinstance(positions, list):
            positions_text = "|".join(str(v).strip() for v in positions if str(v).strip())
        else:
            positions_text = None

        parsed_rows.append(
            {
                "source": row.get("source"),
                "rank": row.get("rank"),
                "player_name": player_name,
                "org": org,
                "level": normalized_level,
                "age": row.get("age"),
                "eta": row.get("eta"),
                "positions": positions_text,
                "bats": row.get("bats"),
                "throws": row.get("throws"),
                "fv": row.get("fv"),
                "ofp": row.get("ofp"),
                "stats_summary": row.get("stats_summary"),
                "scouting_report": row.get("scouting_report"),
                "notes": row.get("notes"),
                "payload_scraped_at": payload.get("scraped_at"),
                "collected_at": collected.isoformat(),
                "source_url": source_url,
            }
        )

    df = pd.DataFrame(parsed_rows)
    if df.empty:
        return df

    for column in ["rank", "age", "ofp"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def collect_prospect_source_rows(
    source_url: str = DEFAULT_PROSPECTS_URL,
    allowed_levels: Iterable[str] = DEFAULT_ALLOWED_LEVELS,
    timeout: int = 30,
) -> pd.DataFrame:
    """Fetch and parse MiLB-only prospect rows from the deployed API."""
    payload = fetch_prospects_payload(source_url=source_url, timeout=timeout)
    return parse_prospects_payload(
        payload=payload,
        source_url=source_url,
        allowed_levels=allowed_levels,
    )
