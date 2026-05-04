from __future__ import annotations

import os
import sys
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def conn():
    pw = os.getenv("PGPASSWORD")
    if not pw:
        print("ERROR: PGPASSWORD missing in env", file=sys.stderr)
        sys.exit(1)

    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        database=os.getenv("PGDATABASE", "sports"),
        user=os.getenv("PGUSER", "postgres"),
        password=pw,
        port=int(os.getenv("PGPORT", "5432")),
    )

RESET_AND_BUILD_SQL = """
CREATE TABLE IF NOT EXISTS ml_features_attendance (
    game_id BIGINT PRIMARY KEY,
    game_date DATE,
    season INT,
    home_team_id INT,
    away_team_id INT,
    venue_id INT,
    attendance INT,
    day_of_week TEXT,
    weekend_flag BOOLEAN,
    month INT,
    start_time_utc TIMESTAMP,
    temperature_f INT,
    precipitation_mm INT,
    wind_kmh INT,
    home_win_pct_5 NUMERIC,
    home_win_pct_10 NUMERIC,
    home_win_pct_20 NUMERIC,
    away_win_pct_5 NUMERIC,
    away_win_pct_10 NUMERIC,
    away_win_pct_20 NUMERIC
);

TRUNCATE TABLE ml_features_attendance;

WITH base_games AS (
    SELECT
        fg.game_id,
        dd.date AS game_date,
        EXTRACT(YEAR FROM dd.date)::INT AS season,
        fg.home_team_id,
        fg.away_team_id,
        fg.venue_id,
        fg.attendance,
        dd.day_of_week,
        dd.weekend_flag,
        EXTRACT(MONTH FROM dd.date)::INT AS month,
        fg.start_time AS start_time_utc,
        fw.temperature_f AS temperature_f,
        fw.precipitation AS precipitation_mm,
        fw.wind_speed AS wind_kmh,
        fg.home_score,
        fg.away_score
    FROM fact_game fg
    JOIN dim_date dd ON dd.date_id = fg.date_id
    LEFT JOIN fact_weather fw ON fw.game_id = fg.game_id
    WHERE fg.attendance IS NOT NULL
        AND fg.attendance > 0
        AND dd.date BETWEEN '2025-03-20' AND '2025-10-05'
      AND fg.home_score IS NOT NULL
      AND fg.away_score IS NOT NULL
),
team_results AS (
    SELECT
        game_id,
        game_date,
        season,
        venue_id,
        attendance,
        day_of_week,
        weekend_flag,
        month,
        start_time_utc,
        temperature_f,
        precipitation_mm,
        wind_kmh,
        home_team_id AS team_id,
        CASE WHEN home_score > away_score THEN 1 ELSE 0 END AS win
    FROM base_games

    UNION ALL

    SELECT
        game_id,
        game_date,
        season,
        venue_id,
        attendance,
        day_of_week,
        weekend_flag,
        month,
        start_time_utc,
        temperature_f,
        precipitation_mm,
        wind_kmh,
        away_team_id AS team_id,
        CASE WHEN away_score > home_score THEN 1 ELSE 0 END AS win
    FROM base_games
),
rolling AS (
    SELECT
        tr.*,
        AVG(win) OVER (
          PARTITION BY team_id
          ORDER BY game_date, game_id
          ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS win_pct_5,
        AVG(win) OVER (
          PARTITION BY team_id
          ORDER BY game_date, game_id
          ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS win_pct_10,
        AVG(win) OVER (
          PARTITION BY team_id
          ORDER BY game_date, game_id
          ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS win_pct_20
    FROM team_results tr
),
pivoted AS (
    SELECT
        bg.game_id,
        bg.game_date,
        bg.season,
        bg.home_team_id,
        bg.away_team_id,
        bg.venue_id,
        bg.attendance,
        bg.day_of_week,
        bg.weekend_flag,
        bg.month,
        bg.start_time_utc,
        bg.temperature_f,
        bg.precipitation_mm,
        bg.wind_kmh,
        ROUND(MAX(CASE WHEN r.team_id = bg.home_team_id THEN r.win_pct_5 END), 3)  AS home_win_pct_5,
        ROUND(MAX(CASE WHEN r.team_id = bg.home_team_id THEN r.win_pct_10 END), 3) AS home_win_pct_10,
        ROUND(MAX(CASE WHEN r.team_id = bg.home_team_id THEN r.win_pct_20 END), 3) AS home_win_pct_20,

        ROUND(MAX(CASE WHEN r.team_id = bg.away_team_id THEN r.win_pct_5 END), 3)  AS away_win_pct_5,
        ROUND(MAX(CASE WHEN r.team_id = bg.away_team_id THEN r.win_pct_10 END), 3) AS away_win_pct_10,
        ROUND(MAX(CASE WHEN r.team_id = bg.away_team_id THEN r.win_pct_20 END), 3) AS away_win_pct_20
    FROM base_games bg
    LEFT JOIN rolling r ON r.game_id = bg.game_id
    GROUP BY
        bg.game_id, bg.game_date, bg.season, bg.home_team_id, bg.away_team_id,
        bg.venue_id, bg.attendance, bg.day_of_week, bg.weekend_flag, bg.month,
        bg.start_time_utc, bg.temperature_f, bg.precipitation_mm, bg.wind_kmh
)
INSERT INTO ml_features_attendance (
  game_id, game_date, season,
  home_team_id, away_team_id, venue_id,
  attendance,
  day_of_week, weekend_flag, month,
  start_time_utc,
  temperature_f, precipitation_mm, wind_kmh,
  home_win_pct_5, home_win_pct_10, home_win_pct_20,
  away_win_pct_5, away_win_pct_10, away_win_pct_20
)
SELECT
  game_id, game_date, season,
  home_team_id, away_team_id, venue_id,
  attendance,
  day_of_week, weekend_flag, month,
  start_time_utc,
  temperature_f, precipitation_mm, wind_kmh,
  home_win_pct_5, home_win_pct_10, home_win_pct_20,
  away_win_pct_5, away_win_pct_10, away_win_pct_20
FROM pivoted;
"""

def main():
    with conn() as c:
        with c.cursor() as cur:
            cur.execute(RESET_AND_BUILD_SQL)
        c.commit()
    print("Built features into ml_features_attendance")

if __name__ == "__main__":
    main()