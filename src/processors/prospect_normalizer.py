"""Normalize MiLB prospect source rows into per-window snapshot rows."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Iterable

import pandas as pd

DEFAULT_WINDOWS: tuple[tuple[str, int | None], ...] = (
    ("STD", None),
    ("30D", 30),
    ("14D", 14),
    ("7D", 7),
)
SOURCE_PRIORITY = {"fangraphs": 0, "prospects_live": 1, "mlb": 2}


def _first_non_null(values: Iterable[Any]) -> Any:
    for value in values:
        if pd.notna(value):
            return value
    return None


def _source_priority(source: Any) -> int:
    return SOURCE_PRIORITY.get(str(source).strip().lower(), len(SOURCE_PRIORITY))


def _collapse_sources(source_rows: pd.DataFrame) -> pd.DataFrame:
    if source_rows.empty:
        return source_rows.copy()

    grouped_records: list[dict[str, Any]] = []
    grouped = source_rows.groupby(["player_name"], dropna=False, sort=False)

    for (player_name,), group in grouped:
        ordered = group.assign(_source_priority=group["source"].map(_source_priority)).sort_values(
            by=["_source_priority"],
            kind="stable",
        )
        source_rank_map = {
            str(source): rank
            for source, rank in group[["source", "rank"]].dropna(subset=["source"]).itertuples(index=False)
        }

        rank_values = pd.to_numeric(group["rank"], errors="coerce").dropna()
        org = _first_non_null(ordered["org"])
        level = _first_non_null(ordered["level"])

        grouped_records.append(
            {
                "player_name": player_name,
                "org": org,
                "level": level,
                "positions": _first_non_null(ordered["positions"]),
                "age": _first_non_null(ordered["age"]),
                "eta": _first_non_null(ordered["eta"]),
                "bats": _first_non_null(ordered["bats"]),
                "throws": _first_non_null(ordered["throws"]),
                "fv": _first_non_null(ordered["fv"]),
                "ofp": _first_non_null(ordered["ofp"]),
                "stats_summary": _first_non_null(ordered["stats_summary"]),
                "scouting_report": _first_non_null(ordered["scouting_report"]),
                "notes": _first_non_null(ordered["notes"]),
                "payload_scraped_at": _first_non_null(ordered["payload_scraped_at"]),
                "collected_at": _first_non_null(ordered["collected_at"]),
                "source_url": _first_non_null(ordered["source_url"]),
                "source_count": int(group.shape[0]),
                "mlb_rank": source_rank_map.get("mlb"),
                "fangraphs_rank": source_rank_map.get("fangraphs"),
                "prospects_live_rank": source_rank_map.get("prospects_live"),
                "best_rank": float(rank_values.min()) if not rank_values.empty else None,
                "avg_rank": float(rank_values.mean()) if not rank_values.empty else None,
            }
        )

    collapsed = pd.DataFrame(grouped_records)
    collapsed["best_rank"] = pd.to_numeric(collapsed["best_rank"], errors="coerce")
    collapsed = collapsed.sort_values(by=["best_rank", "player_name"], na_position="last")
    return collapsed.reset_index(drop=True)


def build_prospects_snapshot(
    source_rows: pd.DataFrame,
    windows: Iterable[tuple[str, int | None]] = DEFAULT_WINDOWS,
    stats_rows: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Expand MiLB prospect rows into season/rolling-window snapshot rows."""
    if source_rows.empty:
        return source_rows.copy()

    collapsed = _collapse_sources(source_rows)
    window_df = pd.DataFrame(list(windows), columns=["window", "window_days"])

    expanded = collapsed.merge(window_df, how="cross")

    today = date.today()
    expanded["window_start"] = expanded["window_days"].apply(
        lambda d: None if pd.isna(d) else (today - timedelta(days=int(d))).isoformat()
    )

    expanded["mlbam_id"] = pd.NA
    dynamic_stat_cols: list[str] = []

    if stats_rows is not None and not stats_rows.empty:
        merge_keys = ["player_name", "org", "level", "window"]
        stats_columns = [c for c in stats_rows.columns if c not in merge_keys]
        available_columns = [c for c in stats_rows.columns if c not in merge_keys]
        dynamic_stat_cols = sorted(c for c in available_columns if c != "mlbam_id")

        stats_subset = stats_rows[merge_keys + available_columns].copy()

        if "mlbam_id" in stats_subset.columns:
            expanded = expanded.drop(columns=["mlbam_id"])
        expanded = expanded.merge(stats_subset, on=merge_keys, how="left")

        if "mlbam_id" not in expanded.columns:
            expanded["mlbam_id"] = pd.NA

    ordered_columns = [
        "player_name",
        "org",
        "level",
        "positions",
        "age",
        "eta",
        "bats",
        "throws",
        "fv",
        "ofp",
        "source_count",
        "mlb_rank",
        "fangraphs_rank",
        "prospects_live_rank",
        "best_rank",
        "avg_rank",
        "window",
        "window_days",
        "window_start",
        "mlbam_id",
    ]

    ordered_columns.extend(dynamic_stat_cols)

    ordered_columns.extend(
        [
            "stats_summary",
            "scouting_report",
            "notes",
            "payload_scraped_at",
            "collected_at",
            "source_url",
        ]
    )

    return expanded[ordered_columns]
