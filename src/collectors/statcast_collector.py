import polars as pl
from pybaseball import cache, statcast


def fetch_season_statcast(season_start: str, end_date: str, keep_cols: list[str]) -> pl.DataFrame:
    """Fetch Statcast pitch data for the date range and return a pruned Polars DataFrame."""
    cache.enable()
    raw_pd = statcast(start_dt=season_start, end_dt=end_date, verbose=True)

    if raw_pd.empty:
        return pl.DataFrame(schema={col: pl.Utf8 for col in keep_cols})

    raw = pl.from_pandas(raw_pd).select(keep_cols)
    return raw
