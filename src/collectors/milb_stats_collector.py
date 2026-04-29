"""Collect true MiLB window stats for a prospect player list.

Uses MLB StatsAPI as the data source, resolves player IDs from names, and
pulls date-range hitting and pitching stats from the MiLB-only player pool.
"""

from __future__ import annotations

from datetime import date, timedelta
import re
from typing import Any, Iterable, Mapping
import unicodedata

import pandas as pd
import requests

DEFAULT_MILB_SPORT_IDS: tuple[int, ...] = (11, 12, 13, 14)


def _canonical_name(value: Any) -> str:
    """Return a normalized name token for cross-endpoint matching."""
    if value is None:
        return ""

    text = str(value).strip()
    if not text:
        return ""

    folded = unicodedata.normalize("NFKD", text)
    without_marks = "".join(char for char in folded if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]", "", without_marks.casefold())


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _normalize_stat_value(value: Any) -> Any:
    """Normalize scalar values from StatsAPI payloads for dataframe storage."""
    if isinstance(value, str):
        text = value.strip()
        if text in {"", "-.--", ".---", "-.-"}:
            return pd.NA
        return text
    return value


def resolve_mlbam_id(
    player_name: str,
    timeout: int = 30,
    session: requests.Session | None = None,
) -> int | None:
    """Resolve MLBAM person id using StatsAPI person search."""
    if not player_name.strip():
        return None

    client = session or requests
    response = client.get(
        "https://statsapi.mlb.com/api/v1/people/search",
        params={"names": player_name},
        timeout=timeout,
    )
    response.raise_for_status()

    payload = response.json()
    people = payload.get("people") if isinstance(payload, Mapping) else None
    if not isinstance(people, list) or not people:
        return None

    # StatsAPI returns best match first for exact/full-name searches.
    return _safe_int(people[0].get("id"))


def fetch_milb_group_pool_stats(
    group: str,
    start_date: date,
    end_date: date,
    sport_ids: Iterable[int] = DEFAULT_MILB_SPORT_IDS,
    limit: int = 20000,
    timeout: int = 30,
    session: requests.Session | None = None,
) -> dict[int, dict[str, Any]]:
    """Fetch MiLB group stats for the full player pool over a date range."""
    client = session or requests
    sport_ids_list = [int(v) for v in sport_ids]

    response = client.get(
        "https://statsapi.mlb.com/api/v1/stats",
        params={
            "stats": "byDateRange",
            "group": group,
            "playerPool": "ALL",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "sportIds": ",".join(str(v) for v in sport_ids_list),
            "limit": int(limit),
        },
        timeout=timeout,
    )
    response.raise_for_status()

    payload = response.json()
    stats_nodes = payload.get("stats") if isinstance(payload, Mapping) else None
    if not isinstance(stats_nodes, list) or not stats_nodes:
        return {}

    splits = stats_nodes[0].get("splits")
    if not isinstance(splits, list) or not splits:
        return {}

    results: dict[int, dict[str, Any]] = {}
    for split in splits:
        player_node = split.get("player")
        if not isinstance(player_node, Mapping):
            continue
        player_id = _safe_int(player_node.get("id"))
        if player_id is None:
            continue

        stat_node = split.get("stat")
        if not isinstance(stat_node, Mapping):
            continue

        payload = {
            "mlbam_id": player_id,
            "player_name": player_node.get("fullName"),
        }
        for stat_key, stat_value in stat_node.items():
            payload[stat_key] = _normalize_stat_value(stat_value)

        # One split per player is currently returned for this endpoint/window.
        results[player_id] = payload

    return results


def collect_prospect_window_stats(
    source_rows: pd.DataFrame,
    windows: Iterable[tuple[str, int | None]],
    as_of_date: date | None = None,
    sport_ids: Iterable[int] = DEFAULT_MILB_SPORT_IDS,
    timeout: int = 30,
) -> pd.DataFrame:
    """Collect MiLB window stats for each unique prospect identity row."""
    if source_rows.empty:
        return pd.DataFrame()

    as_of = as_of_date or date.today()
    identities = (
        source_rows[["player_name", "org", "level"]]
        .drop_duplicates()
        .sort_values(by=["player_name", "org", "level"])
    )

    records: list[dict[str, Any]] = []
    id_cache: dict[str, int | None] = {}

    with requests.Session() as session:
        def _is_missing(value: Any) -> bool:
            return bool(pd.isna(value))

        def _merge_payload(record: dict[str, Any], payload: dict[str, Any], group: str) -> None:
            for key, value in payload.items():
                if key in {"mlbam_id", "player_name"}:
                    continue

                if key not in record or _is_missing(record[key]):
                    record[key] = value
                    continue

                existing = record[key]
                if _is_missing(existing) and not _is_missing(value):
                    record[key] = value
                    continue

                if not _is_missing(value) and existing != value:
                    # Preserve both values for rare two-way collisions.
                    record[f"{key}_{group}"] = value

        def _build_name_index(pool: Mapping[int, Mapping[str, Any]]) -> dict[str, set[int]]:
            name_index: dict[str, set[int]] = {}
            for player_id, payload in pool.items():
                canonical = _canonical_name(payload.get("player_name"))
                if not canonical:
                    continue
                name_index.setdefault(canonical, set()).add(int(player_id))
            return name_index

        for identity in identities.itertuples(index=False):
            player_name, org, level = identity
            if player_name not in id_cache:
                try:
                    id_cache[player_name] = resolve_mlbam_id(
                        player_name=player_name,
                        timeout=timeout,
                        session=session,
                    )
                except requests.RequestException:
                    id_cache[player_name] = None

            mlbam_id = id_cache[player_name]

            for window_label, window_days in windows:
                metric_row: dict[str, Any] = {
                    "player_name": player_name,
                    "org": org,
                    "level": level,
                    "window": window_label,
                    "mlbam_id": mlbam_id,
                    "_canonical_name": _canonical_name(player_name),
                }

                records.append(metric_row)

        for window_label, window_days in windows:
            if window_days is None:
                window_start = date(as_of.year, 1, 1)
            else:
                window_start = as_of - timedelta(days=int(window_days))

            try:
                hitting_pool = fetch_milb_group_pool_stats(
                    group="hitting",
                    start_date=window_start,
                    end_date=as_of,
                    sport_ids=sport_ids,
                    timeout=timeout,
                    session=session,
                )
            except requests.RequestException:
                hitting_pool = {}

            try:
                pitching_pool = fetch_milb_group_pool_stats(
                    group="pitching",
                    start_date=window_start,
                    end_date=as_of,
                    sport_ids=sport_ids,
                    timeout=timeout,
                    session=session,
                )
            except requests.RequestException:
                pitching_pool = {}

            hitting_name_index = _build_name_index(hitting_pool)
            pitching_name_index = _build_name_index(pitching_pool)

            for record in records:
                if record["window"] != window_label:
                    continue
                mlbam_id = record.get("mlbam_id")

                if mlbam_id is None:
                    canonical_name = record.get("_canonical_name")
                    candidates = set()
                    if canonical_name:
                        candidates.update(hitting_name_index.get(canonical_name, set()))
                        candidates.update(pitching_name_index.get(canonical_name, set()))

                    if len(candidates) == 1:
                        mlbam_id = int(next(iter(candidates)))
                        record["mlbam_id"] = mlbam_id

                if mlbam_id is None:
                    continue

                hit_payload = hitting_pool.get(int(mlbam_id))
                if hit_payload:
                    _merge_payload(record, hit_payload, "hitting")

                pitch_payload = pitching_pool.get(int(mlbam_id))
                if pitch_payload:
                    _merge_payload(record, pitch_payload, "pitching")

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records)
    if "_canonical_name" in result.columns:
        result = result.drop(columns=["_canonical_name"])
    return result
