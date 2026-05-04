import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from dotenv import load_dotenv


MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
MLB_GAMEFEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
MLB_VENUES_URL = "https://statsapi.mlb.com/api/v1/venues"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

VENUE_COORDS_FALLBACK = {
    # "T-Mobile Park": (47.5914, -122.3325),
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--team", default=None, help='Optional full team name, like "Seattle Mariners"')
    parser.add_argument("--sleep-ms", type=int, default=120, help="Pause between API calls")
    return parser.parse_args()


def get_connection():
    load_dotenv()

    password = os.getenv("PGPASSWORD")
    if not password:
        print("ERROR: PGPASSWORD is missing from .env", file=sys.stderr)
        sys.exit(1)

    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        database=os.getenv("PGDATABASE", "sports"),
        user=os.getenv("PGUSER", "postgres"),
        password=password,
        port=int(os.getenv("PGPORT", "5432")),
    )


def get_json(url, params=None):
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def to_utc_datetime(iso_str):
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(timezone.utc)


def day_name(d):
    return d.strftime("%A")


def is_weekend(d):
    return d.weekday() >= 5


def ensure_tables_ready(cur):
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_team_team_name ON dim_team (team_name);")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_venue_venue_name ON dim_venue (venue_name);")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_date_date ON dim_date (date);")

    cur.execute("ALTER TABLE dim_venue ADD COLUMN IF NOT EXISTS latitude NUMERIC;")
    cur.execute("ALTER TABLE dim_venue ADD COLUMN IF NOT EXISTS longitude NUMERIC;")

    cur.execute("ALTER TABLE fact_game ADD COLUMN IF NOT EXISTS home_score INT;")
    cur.execute("ALTER TABLE fact_game ADD COLUMN IF NOT EXISTS away_score INT;")


def upsert_team(cur, team_name):
    cur.execute(
        """
        INSERT INTO dim_team (team_name, league)
        VALUES (%s, 'MLB')
        ON CONFLICT (team_name) DO UPDATE
        SET league = EXCLUDED.league
        RETURNING team_id;
        """,
        (team_name,),
    )
    return cur.fetchone()[0]


def upsert_date(cur, game_date):
    cur.execute(
        """
        INSERT INTO dim_date (date, day_of_week, weekend_flag, holiday_flag)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
        SET day_of_week = EXCLUDED.day_of_week,
            weekend_flag = EXCLUDED.weekend_flag
        RETURNING date_id;
        """,
        (game_date, day_name(game_date), is_weekend(game_date), False),
    )
    return cur.fetchone()[0]


def upsert_venue(cur, venue_name, city, state, latitude, longitude):
    cur.execute(
        """
        INSERT INTO dim_venue (venue_name, city, state, indoor_flag, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (venue_name) DO UPDATE
        SET city = COALESCE(EXCLUDED.city, dim_venue.city),
            state = COALESCE(EXCLUDED.state, dim_venue.state),
            latitude = COALESCE(EXCLUDED.latitude, dim_venue.latitude),
            longitude = COALESCE(EXCLUDED.longitude, dim_venue.longitude)
        RETURNING venue_id;
        """,
        (venue_name, city, state, None, latitude, longitude),
    )
    return cur.fetchone()[0]


def get_venue_coords(cur, venue_id):
    cur.execute("SELECT latitude, longitude FROM dim_venue WHERE venue_id = %s;", (venue_id,))
    row = cur.fetchone()
    if not row:
        return None, None

    lat, lon = row
    return float(lat) if lat is not None else None, float(lon) if lon is not None else None


def upsert_game(cur, game_id, date_id, home_team_id, away_team_id, venue_id, attendance, home_score, away_score, start_time):
    cur.execute(
        """
        INSERT INTO fact_game (
            game_id, date_id, home_team_id, away_team_id, venue_id,
            attendance, home_score, away_score, start_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO UPDATE
        SET date_id = EXCLUDED.date_id,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            venue_id = EXCLUDED.venue_id,
            attendance = EXCLUDED.attendance,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            start_time = EXCLUDED.start_time;
        """,
        (game_id, date_id, home_team_id, away_team_id, venue_id, attendance, home_score, away_score, start_time),
    )


def upsert_weather(cur, game_id, temperature, precipitation, wind_speed):
    cur.execute(
        """
        INSERT INTO fact_weather (game_id, temperature_f, precipitation, wind_speed)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (game_id) DO UPDATE
        SET temperature_f = EXCLUDED.temperature_f,
            precipitation = EXCLUDED.precipitation,
            wind_speed = EXCLUDED.wind_speed;
        """,
        (game_id, temperature, precipitation, wind_speed),
    )


def fetch_schedule(start_date, end_date):
    params = {
        "sportId": 1,
        "startDate": start_date,
        "endDate": end_date,
        "hydrate": "teams,venue"
    }
    data = get_json(MLB_SCHEDULE_URL, params=params)

    games = []
    for day in data.get("dates", []):
        games.extend(day.get("games", []))
    return games


def fetch_game_feed(game_pk):
    return get_json(MLB_GAMEFEED_URL.format(game_pk=game_pk))


def parse_attendance_and_scores(feed):
    attendance = feed.get("gameData", {}).get("gameInfo", {}).get("attendance")

    try:
        attendance = int(str(attendance).replace(",", "").strip()) if attendance is not None else None
    except Exception:
        attendance = None

    teams = feed.get("liveData", {}).get("linescore", {}).get("teams", {})

    try:
        home_score = int(teams.get("home", {}).get("runs")) if teams.get("home", {}).get("runs") is not None else None
    except Exception:
        home_score = None

    try:
        away_score = int(teams.get("away", {}).get("runs")) if teams.get("away", {}).get("runs") is not None else None
    except Exception:
        away_score = None

    return attendance, home_score, away_score


def build_venue_lookup():
    data = get_json(MLB_VENUES_URL, params={"hydrate": "location"})
    lookup = {}

    for venue in data.get("venues", []):
        venue_id = venue.get("id")
        if venue_id is None:
            continue

        location = venue.get("location", {}) or {}
        default_coords = location.get("defaultCoordinates", {}) or {}

        lat = location.get("latitude", default_coords.get("latitude"))
        lon = location.get("longitude", default_coords.get("longitude"))
        city = location.get("city")
        state = location.get("state") or location.get("stateAbbrev")

        try:
            lat = float(lat) if lat is not None else None
        except Exception:
            lat = None

        try:
            lon = float(lon) if lon is not None else None
        except Exception:
            lon = None

        lookup[int(venue_id)] = (lat, lon, city, state)

    return lookup


def fetch_weather(latitude, longitude, game_time_utc):
    if latitude is None or longitude is None:
        return None, None, None

    start_date = (game_time_utc - timedelta(hours=3)).date().isoformat()
    end_date = (game_time_utc + timedelta(hours=3)).date().isoformat()

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "timezone": "UTC",
    }

    data = get_json(OPEN_METEO_URL, params=params)
    hourly = data.get("hourly", {})

    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    precip = hourly.get("precipitation", [])
    wind = hourly.get("windspeed_10m", [])

    if not times:
        return None, None, None

    target_hour = game_time_utc.replace(minute=0, second=0, microsecond=0)
    target_prefix = target_hour.strftime("%Y-%m-%dT%H")

    match_index = None
    for i, t in enumerate(times):
        if t.startswith(target_prefix):
            match_index = i
            break

    if match_index is None:
        closest_diff = None
        for i, t in enumerate(times):
            try:
                weather_time = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
            except Exception:
                continue

            diff = abs((weather_time - game_time_utc).total_seconds())
            if closest_diff is None or diff < closest_diff:
                closest_diff = diff
                match_index = i

    if match_index is None:
        return None, None, None

    def safe_value(values, idx):
        try:
            value = values[idx]
            return float(value) if value is not None else None
        except Exception:
            return None

    temp_c = safe_value(temps, match_index)
    temp_f = (temp_c * 9/5 + 32) if temp_c is not None else None

    precip_val = safe_value(precip, match_index)
    wind_val = safe_value(wind, match_index)

    # Round everything to whole numbers
    temp_f = round(temp_f) if temp_f is not None else None
    precip_val = round(precip_val) if precip_val is not None else None
    wind_val = round(wind_val) if wind_val is not None else None

    return (
        temp_f,
        precip_val,
        wind_val,
    )
def main():
    args = parse_args()
    games = fetch_schedule(args.start_date, args.end_date)

    if args.team:
        target_team = args.team.strip().lower()
        games = [
            g for g in games
            if g.get("teams", {}).get("home", {}).get("team", {}).get("name", "").strip().lower() == target_team
            or g.get("teams", {}).get("away", {}).get("team", {}).get("name", "").strip().lower() == target_team
        ]

    print(f"Found {len(games)} games between {args.start_date} and {args.end_date}")

    venue_lookup = build_venue_lookup()
    conn = get_connection()
    conn.autocommit = False

    loaded = 0
    skipped_attendance = 0
    missing_weather = 0
    missing_coords = 0

    try:
        with conn.cursor() as cur:
            ensure_tables_ready(cur)

            for idx, game in enumerate(games, start=1):
                game_pk = game.get("gamePk")
                game_date_str = game.get("gameDate")

                if not game_pk or not game_date_str:
                    continue

                game_pk = int(game_pk)
                start_time_utc = to_utc_datetime(game_date_str)
                game_day = start_time_utc.date()

                home_name = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "Unknown Home")
                away_name = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "Unknown Away")

                venue = game.get("venue", {}) or {}
                venue_name = venue.get("name", "Unknown Venue")
                venue_api_id = venue.get("id")

                lat = lon = city = state = None
                if venue_api_id in venue_lookup:
                    lat, lon, city, state = venue_lookup[venue_api_id]

                if (lat is None or lon is None) and venue_name in VENUE_COORDS_FALLBACK:
                    lat, lon = VENUE_COORDS_FALLBACK[venue_name]

                feed = fetch_game_feed(game_pk)
                attendance, home_score, away_score = parse_attendance_and_scores(feed)

                if attendance is None or attendance <= 0:
                    skipped_attendance += 1
                    continue

                date_id = upsert_date(cur, game_day)
                home_team_id = upsert_team(cur, home_name)
                away_team_id = upsert_team(cur, away_name)
                venue_id = upsert_venue(cur, venue_name, city, state, lat, lon)

                db_lat, db_lon = get_venue_coords(cur, venue_id)

                upsert_game(
                    cur,
                    game_pk,
                    date_id,
                    home_team_id,
                    away_team_id,
                    venue_id,
                    attendance,
                    home_score,
                    away_score,
                    start_time_utc,
                )

                if db_lat is None or db_lon is None:
                    temp_c, precip_mm, wind_kmh = None, None, None
                    missing_coords += 1
                else:
                    temp_c, precip_mm, wind_kmh = fetch_weather(db_lat, db_lon, start_time_utc)
                    if temp_c is None and precip_mm is None and wind_kmh is None:
                        missing_weather += 1

                upsert_weather(cur, game_pk, temp_c, precip_mm, wind_kmh)

                conn.commit()
                loaded += 1

                if idx % 25 == 0:
                    print(f"Progress: {idx}/{len(games)} games checked, {loaded} loaded")

                if args.sleep_ms > 0:
                    time.sleep(args.sleep_ms / 1000.0)

        print("ETL completed")
        print(f"Games loaded: {loaded}")
        print(f"Games skipped for missing attendance: {skipped_attendance}")
        print(f"Games missing venue coords: {missing_coords}")
        print(f"Games missing weather: {missing_weather}")

    except Exception:
        conn.rollback()
        print("ETL failed, transaction rolled back", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()