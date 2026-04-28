"""Collector for deployed Yahoo app prospects API.

This module fetches and parses the prospects payload exposed by the deployed app,
resolves conflicting player profile fields across sources, then filters rows to
explicit MiLB levels only.
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
SOURCE_PRIORITY = {"fangraphs": 0, "prospects_live": 1, "mlb": 2}
PROFILE_FIELDS: tuple[str, ...] = (
    "org",
    "level",
    "positions",
    "age",
    "eta",
    "bats",
    "throws",
    "fv",
    "ofp",
    "stats_summary",
    "scouting_report",
    "notes",
    "payload_scraped_at",
    "collected_at",
    "source_url",
)


def _first_non_null(values: Iterable[Any]) -> Any:
    for value in values:
        if pd.notna(value):
            return value
    return None


def _source_priority(source: Any) -> int:
    return SOURCE_PRIORITY.get(str(source).strip().lower(), len(SOURCE_PRIORITY))


def _resolve_source_rows(
    source_rows: pd.DataFrame,
    allowed_levels: Iterable[str],
) -> pd.DataFrame:
    if source_rows.empty:
        return source_rows.copy()

    allowed_set = {str(value).upper() for value in allowed_levels}
    retained_groups: list[pd.DataFrame] = []

    # Fangraphs uses org abbreviations while MLB uses full team names, so player
    # name is the only stable cross-source identity available in this payload.
    for _, group in source_rows.groupby(["player_name"], dropna=False, sort=False):
        ordered = group.sort_values(
            by=["_source_priority", "_row_order"],
            kind="stable",
        )

        resolved_fields = {
            field: _first_non_null(ordered[field])
            for field in PROFILE_FIELDS
        }

        resolved_level = resolved_fields["level"]
        if resolved_level is None or str(resolved_level).upper() not in allowed_set:
            continue

        resolved_group = group.copy()
        for field, value in resolved_fields.items():
            resolved_group[field] = value
        retained_groups.append(resolved_group)

    if not retained_groups:
        return source_rows.iloc[0:0].drop(columns=["_row_order", "_source_priority"], errors="ignore")

    resolved = pd.concat(retained_groups, ignore_index=True)
    return resolved.drop(columns=["_row_order", "_source_priority"], errors="ignore")


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
    """Parse prospect rows, resolve source conflicts, and keep MiLB-only winners."""
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError("Prospects payload is missing rows")

    collected = collected_at or datetime.now(timezone.utc)

    parsed_rows: list[dict[str, Any]] = []
    for row_order, row in enumerate(rows):
        if not isinstance(row, Mapping):
            continue

        normalized_level = normalize_level(row.get("level"))

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
                "_row_order": row_order,
                "_source_priority": _source_priority(row.get("source")),
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

    return _resolve_source_rows(df, allowed_levels=allowed_levels)


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
