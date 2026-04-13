import requests
import polars as pl
from datetime import date


_MLB_API_BASE = "https://statsapi.mlb.com/api/v1"


def fetch_boxscore_batting_stats(start_date: str, end_date: str) -> pl.DataFrame:
    """Fetch per-player per-game BB, K, and SB from the official MLB Stats API.

    Queries the schedule for all final regular-season games in the date range,
    then fetches each game's boxscore and extracts batting stats for every batter.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date:   End date in YYYY-MM-DD format (inclusive).

    Returns:
        DataFrame with columns: game_date (Date), mlbam_id (Int32),
        bb (Int32), k (Int32), sb (Int32).
    """
    empty = pl.DataFrame(
        schema={
            "game_date": pl.Date,
            "mlbam_id": pl.Int32,
            "bb": pl.Int32,
            "k": pl.Int32,
            "sb": pl.Int32,
        }
    )

    start_dt = date.fromisoformat(start_date)
    end_dt = date.fromisoformat(end_date)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str = end_dt.strftime("%m/%d/%Y")

    schedule_resp = requests.get(
        f"{_MLB_API_BASE}/schedule",
        params={
            "sportId": 1,
            "startDate": start_str,
            "endDate": end_str,
            "gameType": "R",
        },
        timeout=30,
    )
    schedule_resp.raise_for_status()
    schedule = schedule_resp.json()

    game_dates: list[tuple[str, int]] = []
    for date_entry in schedule.get("dates", []):
        for game in date_entry.get("games", []):
            if game.get("status", {}).get("abstractGameState") == "Final":
                game_dates.append((date_entry["date"], game["gamePk"]))

    if not game_dates:
        return empty

    records: list[dict] = []
    for game_date_str, game_pk in game_dates:
        box_resp = requests.get(
            f"{_MLB_API_BASE}/game/{game_pk}/boxscore",
            timeout=30,
        )
        if box_resp.status_code != 200:
            continue
        box = box_resp.json()

        for side in ("home", "away"):
            team = box.get("teams", {}).get(side, {})
            players = team.get("players", {})
            for player_id in team.get("batters", []):
                player = players.get(f"ID{player_id}", {})
                batting = player.get("stats", {}).get("batting", {})
                if not batting:
                    continue
                records.append(
                    {
                        "game_date": game_date_str,
                        "mlbam_id": player_id,
                        "bb": batting.get("baseOnBalls", 0)
                        + batting.get("intentionalWalks", 0),
                        "k": batting.get("strikeOuts", 0),
                        "sb": batting.get("stolenBases", 0),
                    }
                )

    if not records:
        return empty

    return (
        pl.DataFrame(records)
        .with_columns(pl.col("game_date").str.to_date())
        .cast({"mlbam_id": pl.Int32, "bb": pl.Int32, "k": pl.Int32, "sb": pl.Int32})
    )
