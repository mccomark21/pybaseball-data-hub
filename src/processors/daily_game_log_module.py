from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

import polars as pl

from src.processors.metric_calculator import aggregate_batter_game_stats

RunMode = Literal["daily", "full_season"]
RunStatus = Literal["written", "no_data"]


class DailyGameLogError(Exception):
    """Base error for daily game-log workflow failures."""


class ExternalFetchError(DailyGameLogError):
    """Raised when an external data source cannot be fetched."""

    def __init__(self, source: str, message: str):
        super().__init__(f"{source} fetch failed: {message}")
        self.source = source


class PersistenceError(DailyGameLogError):
    """Raised when parquet read/write persistence fails."""


class StatcastAdapter(Protocol):
    def fetch(self, start_date: str, end_date: str, keep_cols: list[str]) -> pl.DataFrame:
        """Fetch Statcast rows for the requested date window."""


class MlbBoxscoreAdapter(Protocol):
    def fetch(self, start_date: str, end_date: str) -> pl.DataFrame:
        """Fetch per-player, per-game batting boxscore counts."""


class DefaultStatcastAdapter:
    def fetch(self, start_date: str, end_date: str, keep_cols: list[str]) -> pl.DataFrame:
        from src.collectors.statcast_collector import fetch_season_statcast

        return fetch_season_statcast(start_date=start_date, end_date=end_date, keep_cols=keep_cols)


class DefaultMlbBoxscoreAdapter:
    def fetch(self, start_date: str, end_date: str) -> pl.DataFrame:
        from src.collectors.mlb_api_collector import fetch_boxscore_batting_stats

        return fetch_boxscore_batting_stats(start_date=start_date, end_date=end_date)


@dataclass(frozen=True)
class DailyGameLogConfig:
    season_start: str
    keep_cols: list[str]
    pull_threshold: int
    game_log_path: Path
    daily_lookback_days: int = 3
    timezone_name: str = "America/New_York"


@dataclass(frozen=True)
class DailyGameLogRequest:
    mode: RunMode = "daily"
    as_of: date | None = None


@dataclass(frozen=True)
class DailyGameLogResult:
    status: RunStatus
    mode: RunMode
    start_date: date
    end_date: date
    batch_rows: int
    total_rows: int
    output_path: Path


class DailyGameLogModule:
    """Deep workflow module for building and persisting batter game-log rows."""

    def __init__(
        self,
        config: DailyGameLogConfig,
        statcast_adapter: StatcastAdapter | None = None,
        boxscore_adapter: MlbBoxscoreAdapter | None = None,
    ) -> None:
        self._config = config
        self._statcast = statcast_adapter or DefaultStatcastAdapter()
        self._boxscore = boxscore_adapter or DefaultMlbBoxscoreAdapter()

    def refresh(self, request: DailyGameLogRequest) -> DailyGameLogResult:
        start_dt, end_dt = self._resolve_window(request)
        start_str = start_dt.isoformat()
        end_str = end_dt.isoformat()

        try:
            raw = self._statcast.fetch(start_str, end_str, self._config.keep_cols)
        except Exception as exc:  # pragma: no cover - network adapter branch
            raise ExternalFetchError("statcast", str(exc)) from exc

        if raw.is_empty():
            return DailyGameLogResult(
                status="no_data",
                mode=request.mode,
                start_date=start_dt,
                end_date=end_dt,
                batch_rows=0,
                total_rows=self._current_row_count_or_zero(),
                output_path=self._config.game_log_path,
            )

        statcast_agg = aggregate_batter_game_stats(
            raw=raw.lazy(),
            pull_threshold=self._config.pull_threshold,
        ).collect()

        try:
            mlb_stats = self._boxscore.fetch(start_str, end_str)
        except Exception as exc:  # pragma: no cover - network adapter branch
            raise ExternalFetchError("mlb_boxscore", str(exc)) from exc

        batch = (
            statcast_agg.lazy()
            .join(mlb_stats.lazy(), on=["mlbam_id", "game_date"], how="left")
            .with_columns([
                pl.col("bb").fill_null(0),
                pl.col("k").fill_null(0),
                pl.col("sb").fill_null(0),
            ])
            .select([
                "game_date",
                "mlbam_id",
                "season",
                "pa",
                "bbe",
                "xwoba_num",
                "xwoba_denom",
                "pull_air_events",
                "bb",
                "k",
                "sb",
            ])
            .collect()
        )

        total_rows = self._persist(batch=batch, mode=request.mode)
        return DailyGameLogResult(
            status="written",
            mode=request.mode,
            start_date=start_dt,
            end_date=end_dt,
            batch_rows=batch.height,
            total_rows=total_rows,
            output_path=self._config.game_log_path,
        )

    def _resolve_window(self, request: DailyGameLogRequest) -> tuple[date, date]:
        as_of = request.as_of
        if as_of is None:
            tz = ZoneInfo(self._config.timezone_name)
            as_of = datetime.now(tz).date()

        end_dt = as_of - timedelta(days=1)
        if request.mode == "full_season":
            start_dt = date.fromisoformat(self._config.season_start)
        else:
            start_dt = as_of - timedelta(days=self._config.daily_lookback_days)

        return start_dt, end_dt

    def _persist(self, batch: pl.DataFrame, mode: RunMode) -> int:
        target_path = self._config.game_log_path
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if mode == "daily" and target_path.exists():
                existing = pl.read_parquet(str(target_path))
                final_frame = (
                    pl.concat([batch, existing], how="vertical_relaxed")
                    .unique(subset=["mlbam_id", "game_date"], keep="first")
                )
            else:
                final_frame = batch

            final_frame.write_parquet(str(target_path))
            return final_frame.height
        except Exception as exc:
            raise PersistenceError(str(exc)) from exc

    def _current_row_count_or_zero(self) -> int:
        path = self._config.game_log_path
        if not path.exists():
            return 0

        try:
            return pl.read_parquet(str(path)).height
        except Exception as exc:
            raise PersistenceError(str(exc)) from exc
