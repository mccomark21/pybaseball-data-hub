"""Daily MiLB prospects snapshot pipeline.

Fetches prospect rows from the deployed app API, keeps MiLB-only levels,
and writes both source rows and 4-window snapshot parquet artifacts.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.collectors.prospects_collector import collect_prospect_source_rows
from src.collectors.milb_stats_collector import collect_prospect_window_stats
from src.processors.prospect_normalizer import DEFAULT_WINDOWS, build_prospects_snapshot

CONFIG_PATH = PROJECT_ROOT / "config" / "prospects_config.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build MiLB prospects parquet snapshots.")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="Path to prospects config YAML.")
    parser.add_argument("--source-url", help="Override source URL.")
    parser.add_argument("--output-path", help="Override snapshot parquet path.")
    parser.add_argument("--raw-output-path", help="Override source rows parquet path.")
    parser.add_argument("--top-n", type=int, help="Optional top-N players by best_rank.")
    parser.add_argument("--season", type=int, help="Optional season tag column in snapshot output.")
    parser.add_argument("--as-of-date", help="Optional YYYY-MM-DD end date for rolling windows.")
    return parser.parse_args()


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as file:
        return yaml.safe_load(file)


def _resolve_windows(window_labels: list[str] | None) -> tuple[tuple[str, int | None], ...]:
    if not window_labels:
        return DEFAULT_WINDOWS

    mapping = {label: days for label, days in DEFAULT_WINDOWS}
    selected: list[tuple[str, int | None]] = []
    for label in window_labels:
        if label not in mapping:
            raise ValueError(f"Unsupported window label: {label}")
        selected.append((label, mapping[label]))
    return tuple(selected)


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    source_url = args.source_url or config["source_url"]
    allowed_levels = config.get("allowed_levels", ["A", "A+", "AA", "AAA"])
    timeout = int(config.get("timeout_seconds", 30))
    sport_ids = config.get("milb_sport_ids", [11, 12, 13, 14])

    raw_output_path = PROJECT_ROOT / (args.raw_output_path or config["raw_output_path"])
    output_path = PROJECT_ROOT / (args.output_path or config["output_path"])

    windows = _resolve_windows(config.get("windows"))

    print(f"Fetching prospects from {source_url}...")
    source_rows = collect_prospect_source_rows(
        source_url=source_url,
        allowed_levels=allowed_levels,
        timeout=timeout,
    )

    raw_output_path.parent.mkdir(parents=True, exist_ok=True)
    source_rows.to_parquet(raw_output_path, index=False)
    print(f"Wrote raw source rows: {raw_output_path} ({len(source_rows):,} rows)")

    stats_as_of = None
    if args.as_of_date:
        stats_as_of = pd.to_datetime(args.as_of_date, format="%Y-%m-%d").date()

    print("Collecting MiLB date-window stats...")
    stats_rows = collect_prospect_window_stats(
        source_rows=source_rows,
        windows=windows,
        as_of_date=stats_as_of,
        sport_ids=sport_ids,
        timeout=timeout,
    )

    snapshot = build_prospects_snapshot(
        source_rows=source_rows,
        windows=windows,
        stats_rows=stats_rows,
    )

    if args.top_n is not None and args.top_n > 0 and not snapshot.empty:
        ranked_players = (
            snapshot[["player_name", "org", "level", "best_rank"]]
            .drop_duplicates()
            .sort_values(by=["best_rank", "player_name"], na_position="last")
            .head(args.top_n)
        )
        snapshot = snapshot.merge(
            ranked_players[["player_name", "org", "level"]],
            on=["player_name", "org", "level"],
            how="inner",
        )

    if args.season is not None:
        snapshot["season"] = args.season

    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot.to_parquet(output_path, index=False)
    print(f"Wrote snapshot: {output_path} ({len(snapshot):,} rows)")

    if not snapshot.empty:
        expected_levels = {str(v).upper() for v in allowed_levels}
        found_levels = {str(v).upper() for v in snapshot["level"].dropna().unique().tolist()}
        if not found_levels.issubset(expected_levels):
            raise RuntimeError(
                "Output includes non-MiLB levels: "
                f"{sorted(found_levels - expected_levels)}"
            )

        window_counts = snapshot.groupby(["player_name", "org", "level"], dropna=False).size()
        if not (window_counts == len(windows)).all():
            raise RuntimeError(
                "Each player must have exactly one row per requested window. "
                f"Found counts: {window_counts.value_counts().to_dict()}"
            )

        player_count = int(window_counts.shape[0])
        print(f"Players retained: {player_count:,}")
        print(f"Windows per player: {window_counts.value_counts().to_dict()}")


if __name__ == "__main__":
    main()
