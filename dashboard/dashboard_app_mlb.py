
from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# --------------------------------------------------
# Config
# --------------------------------------------------
st.set_page_config(
    page_title="MLB Attendance Forecast Dashboard",
    page_icon="⚾",
    layout="wide",
)

load_dotenv()


# --------------------------------------------------
# Database
# --------------------------------------------------
def get_engine():
    pw = os.getenv("PGPASSWORD")
    if not pw:
        st.error("PGPASSWORD is missing. Add your Postgres credentials to your .env file.")
        st.stop()

    url = (
        f"postgresql+psycopg2://{os.getenv('PGUSER', 'postgres')}:{pw}"
        f"@{os.getenv('PGHOST', 'localhost')}:{os.getenv('PGPORT', '5432')}/{os.getenv('PGDATABASE', 'sports')}"
    )
    return create_engine(url, future=True)


@st.cache_data(ttl=300)
def load_base_data() -> pd.DataFrame:
    eng = get_engine()
    query = """
    SELECT
        fg.game_id,
        dd.date AS game_date,
        fg.attendance,
        fg.home_team_id,
        fg.away_team_id,
        fg.venue_id,
        ht.team_name AS home_team,
        at.team_name AS away_team,
        dv.venue_name,
        dv.city,
        dv.state,
        COALESCE(dv.indoor_flag, FALSE) AS indoor_flag,
        fw.temperature_f AS temperature_f,
        fw.precipitation AS precipitation_mm,
        fw.wind_speed AS wind_kmh,
        mf.day_of_week,
        mf.weekend_flag,
        mf.month,
        mf.home_win_pct_10,
        mf.away_win_pct_10
    FROM fact_game fg
    JOIN dim_date dd
      ON dd.date_id = fg.date_id
    JOIN dim_team ht
      ON ht.team_id = fg.home_team_id
    JOIN dim_team at
      ON at.team_id = fg.away_team_id
    JOIN dim_venue dv
      ON dv.venue_id = fg.venue_id
    LEFT JOIN fact_weather fw
      ON fw.game_id = fg.game_id
    LEFT JOIN ml_features_attendance mf
      ON mf.game_id = fg.game_id
    WHERE fg.attendance IS NOT NULL
    ORDER BY dd.date, fg.game_id;
    """
    df = pd.read_sql(query, eng)
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df


@st.cache_data(ttl=300)
def load_forecasts() -> pd.DataFrame:
    eng = get_engine()
    query = """
    SELECT
        faf.game_id,
        faf.model_name,
        faf.run_ts_utc,
        faf.interval_method,
        faf.pred_attendance,
        faf.lo95,
        faf.hi95,
        dd.date AS game_date,
        ht.team_name AS home_team,
        at.team_name AS away_team,
        dv.venue_name,
        dv.city,
        dv.state,
        fw.temperature_f AS temperature_f,
        fw.precipitation AS precipitation_mm,
        fw.wind_speed AS wind_kmh,
        mf.day_of_week,
        mf.weekend_flag,
        mf.home_win_pct_10,
        mf.away_win_pct_10,
        fg.attendance
    FROM fact_attendance_forecast faf
    JOIN fact_game fg
      ON fg.game_id = faf.game_id
    JOIN dim_date dd
      ON dd.date_id = fg.date_id
    JOIN dim_team ht
      ON ht.team_id = fg.home_team_id
    JOIN dim_team at
      ON at.team_id = fg.away_team_id
    JOIN dim_venue dv
      ON dv.venue_id = fg.venue_id
    LEFT JOIN fact_weather fw
      ON fw.game_id = fg.game_id
    LEFT JOIN ml_features_attendance mf
      ON mf.game_id = fg.game_id
    ORDER BY dd.date, faf.game_id;
    """
    df = pd.read_sql(query, eng)
    if df.empty:
        return df

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["run_ts_utc"] = pd.to_datetime(df["run_ts_utc"], errors="coerce", utc=True)
    return df


@st.cache_data(ttl=300)
def load_team_list() -> list[str]:
    df = load_base_data()
    teams = sorted(df["home_team"].dropna().unique().tolist())
    return teams


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def fmt_int(x: Optional[float]) -> str:
    if pd.isna(x):
        return "N/A"
    return f"{int(round(float(x))):,}"


def fmt_num(x: Optional[float], digits: int = 1) -> str:
    if pd.isna(x):
        return "N/A"
    return f"{float(x):.{digits}f}"


def add_risk_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["interval_width"] = out["hi95"] - out["lo95"]
    out["weather_risk_flag"] = (
        (out["precipitation_mm"].fillna(0) >= 1.0)
        | ((out["temperature_f"].fillna(20) <= 10) & (~out["weekend_flag"].fillna(False)))
    )

    out["demand_flag"] = "Normal"
    out.loc[out["pred_attendance"] >= out["pred_attendance"].quantile(0.85), "demand_flag"] = "High Demand"
    out.loc[out["pred_attendance"] <= out["pred_attendance"].quantile(0.15), "demand_flag"] = "Low Demand"

    out["forecast_flag"] = "Monitor"
    out.loc[out["weather_risk_flag"], "forecast_flag"] = "Weather Risk"
    out.loc[(out["demand_flag"] == "High Demand") & (~out["weather_risk_flag"]), "forecast_flag"] = "High Crowd"

    return out

def print_team_debug_stats(df: pd.DataFrame, team_name: str):
    team_df = df[df["home_team"] == team_name].copy()

    if team_df.empty:
        print(f"No data found for {team_name}")
        return

    print("\n==============================")
    print(f"DEBUG STATS FOR: {team_name}")
    print("==============================")

    print("\nBasic Info:")
    print(f"Rows: {len(team_df)}")
    print(f"Date range: {team_df['game_date'].min()} to {team_df['game_date'].max()}")

    print("\nAttendance Summary:")
    print(team_df["attendance"].describe())

    print("\nWeekend vs Weekday:")
    weekend_avg = team_df.loc[team_df["weekend_flag"] == True, "attendance"].mean()
    weekday_avg = team_df.loc[team_df["weekend_flag"] == False, "attendance"].mean()
    print(f"Weekend avg: {weekend_avg}")
    print(f"Weekday avg: {weekday_avg}")
    print(f"Lift: {weekend_avg - weekday_avg}")

    print("\nDay of Week:")
    print(
        team_df.groupby("day_of_week")["attendance"]
        .agg(["count", "mean", "median"])
        .sort_values("mean", ascending=False)
    )

    print("\nTop Opponents (by avg attendance):")
    print(
        team_df.groupby("away_team")["attendance"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )

    print("\nCorrelation with Attendance:")
    corr_cols = [
        "attendance",
        "temperature_f",
        "precipitation_mm",
        "wind_kmh",
        "home_win_pct_10",
        "away_win_pct_10",
    ]
    print(team_df[corr_cols].corr(numeric_only=True)["attendance"].sort_values(ascending=False))

    print("\nTemperature Buckets:")
    team_df["temp_bucket"] = pd.cut(
        team_df["temperature_f"],
        bins=[0, 60, 70, 80, 90, 120],
        labels=["<60", "60-70", "70-80", "80-90", "90+"]
    )
    print(team_df.groupby("temp_bucket")["attendance"].mean())

    print("\nWin % Buckets:")
    team_df["win_bucket"] = pd.cut(
        team_df["home_win_pct_10"],
        bins=[0, 0.3, 0.5, 0.7, 1.0],
        labels=["Low", "Below Avg", "Above Avg", "High"]
    )
    print(team_df.groupby("win_bucket")["attendance"].mean())

    print("\n==============================\n")

def style_app():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 1.2rem;
            max-width: 1400px;
        }
        .metric-card {
            background-color: #f7f9fc;
            padding: 1rem 1.1rem;
            border-radius: 16px;
            border: 1px solid #e6ebf2;
            box-shadow: 0 1px 4px rgba(0, 0, 0, 0.04);
        }
        .section-label {
            font-size: 1.05rem;
            font-weight: 600;
            margin-top: 0.4rem;
            margin-bottom: 0.4rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def filter_base_df(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### Filters")

    teams = ["All Teams"] + load_team_list()
    selected_team = st.sidebar.selectbox("Home Team", teams, index=0, key="Home Team")

    min_date = df["game_date"].min().date()
    max_date = df["game_date"].max().date()
    selected_dates = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date, end_date = min_date, max_date

    weekend_only = st.sidebar.checkbox("Weekend games only", value=False)
    indoor_only = st.sidebar.checkbox("Indoor venues only", value=False)

    temp_min = float(df["temperature_f"].dropna().min()) if df["temperature_f"].notna().any() else 0.0
    temp_max = float(df["temperature_f"].dropna().max()) if df["temperature_f"].notna().any() else 40.0
    temp_range = st.sidebar.slider(
        "Temperature (°F)",
        min_value=float(round(temp_min, 1)),
        max_value=float(round(temp_max, 1)),
        value=(float(round(temp_min, 1)), float(round(temp_max, 1))),
    )

    filtered = df.copy()
    filtered = filtered[
        (filtered["game_date"].dt.date >= start_date)
        & (filtered["game_date"].dt.date <= end_date)
    ]

    if selected_team != "All Teams":
        filtered = filtered[filtered["home_team"] == selected_team]

    if weekend_only:
        filtered = filtered[filtered["weekend_flag"] == True]

    if indoor_only:
        filtered = filtered[filtered["indoor_flag"] == True]

    filtered = filtered[
        filtered["temperature_f"].fillna(temp_range[0]).between(temp_range[0], temp_range[1])
    ]

    return filtered


# --------------------------------------------------
# Pages
# --------------------------------------------------
def page_executive_forecast(base_df: pd.DataFrame, forecast_df: pd.DataFrame):
    st.title("MLB Attendance Forecast Dashboard")
    st.caption("Executive forecast view for predicted demand, expected ranges, and game-level risk signals.")

    if forecast_df.empty:
        st.warning("No rows found in fact_attendance_forecast yet. Run write_forecasts_to_postgres.py first.")
        return

    filtered = filter_base_df(forecast_df)
    # DEBUG BUTTON
    # DEBUG BUTTON
    if st.button("Print Debug Stats for Current Filtered View"):
        selected_team = st.session_state.get("Home Team", None)

        if selected_team is None or selected_team == "All Teams":
            selected_team = "Arizona Diamondbacks"

        print_team_debug_stats(filtered, selected_team)

    filtered = add_risk_flags(filtered)

    if filtered.empty:
        st.info("No forecast rows match the selected filters.")
        return

    latest_run = filtered["run_ts_utc"].max()

    # KPI row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Games in View", fmt_int(len(filtered)))
    c2.metric("Avg Predicted Attendance", fmt_int(filtered["pred_attendance"].mean()))
    c3.metric("Highest Predicted Game", fmt_int(filtered["pred_attendance"].max()))
    c4.metric("Avg Expected Range Width", fmt_int((filtered["hi95"] - filtered["lo95"]).mean()))

    st.markdown(
        f"**Latest forecast run:** {latest_run.strftime('%Y-%m-%d %H:%M UTC') if pd.notna(latest_run) else 'N/A'}"
    )

    # Daily summary for cleaner chart
    daily = (
        filtered.groupby("game_date", as_index=False)
        .agg(
            avg_pred_attendance=("pred_attendance", "mean"),
            avg_lo95=("lo95", "mean"),
            avg_hi95=("hi95", "mean"),
            games=("game_id", "count"),
        )
        .sort_values("game_date")
    )

    st.markdown("### Daily Forecast Trend")
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=daily["game_date"],
            y=daily["avg_hi95"],
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=daily["game_date"],
            y=daily["avg_lo95"],
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            name="Expected Range",
            hovertemplate="Date: %{x|%Y-%m-%d}<br>Avg Low: %{y:,.0f}<extra></extra>",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=daily["game_date"],
            y=daily["avg_pred_attendance"],
            mode="lines+markers",
            name="Avg Predicted Attendance",
            hovertemplate=(
                "Date: %{x|%Y-%m-%d}<br>"
                "Avg Predicted: %{y:,.0f}<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis_title="Game Date",
        yaxis_title="Average Predicted Attendance",
        legend_title="",
    )

    st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns([1.55, 1])

    with left:
        st.markdown("### Top Priority Games")

        priority_df = filtered[
            (filtered["forecast_flag"] != "Monitor") | (filtered["pred_attendance"] >= filtered["pred_attendance"].quantile(0.85))
        ].copy()

        if priority_df.empty:
            priority_df = filtered.copy()

        priority_df = priority_df.sort_values(
            ["forecast_flag", "pred_attendance"],
            ascending=[True, False]
        ).head(12)

        display_cols = [
            "game_date",
            "home_team",
            "away_team",
            "venue_name",
            "pred_attendance",
            "lo95",
            "hi95",
            "forecast_flag",
        ]

        display_df = priority_df[display_cols].copy()
        display_df["game_date"] = display_df["game_date"].dt.strftime("%Y-%m-%d")

        st.dataframe(
            display_df.rename(
                columns={
                    "game_date": "Game Date",
                    "home_team": "Home Team",
                    "away_team": "Away Team",
                    "venue_name": "Venue",
                    "pred_attendance": "Predicted",
                    "lo95": "Low",
                    "hi95": "High",
                    "forecast_flag": "Flag",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("### Daily Summary Table")
        daily_table = daily.copy()
        daily_table["game_date"] = daily_table["game_date"].dt.strftime("%Y-%m-%d")
        daily_table = daily_table.rename(
            columns={
                "game_date": "Game Date",
                "avg_pred_attendance": "Avg Predicted",
                "avg_lo95": "Avg Low",
                "avg_hi95": "Avg High",
                "games": "Games",
            }
        )
        st.dataframe(daily_table, use_container_width=True, hide_index=True)

    with right:
        st.markdown("### Forecast Flags")
        flag_counts = filtered["forecast_flag"].value_counts().reset_index()
        flag_counts.columns = ["flag", "games"]

        fig_flags = px.bar(flag_counts, x="flag", y="games", title="Games by Flag")
        fig_flags.update_layout(height=300, margin=dict(l=20, r=20, t=50, b=20))
        st.plotly_chart(fig_flags, use_container_width=True)

        st.markdown("### Priority Game Cards")
        card_df = filtered[filtered["forecast_flag"] != "Monitor"].copy()

        if card_df.empty:
            card_df = filtered.sort_values("pred_attendance", ascending=False).head(5)
        else:
            card_df = card_df.sort_values("pred_attendance", ascending=False).head(5)

        for _, row in card_df.iterrows():
            st.markdown(
                f"""
                <div class="metric-card">
                    <b>{row['home_team']} vs {row['away_team']}</b><br>
                    {row['game_date'].strftime('%Y-%m-%d')}<br>
                    Venue: {row['venue_name']}<br>
                    Predicted: {fmt_int(row['pred_attendance'])}<br>
                    Expected range: {fmt_int(row['lo95'])} to {fmt_int(row['hi95'])}<br>
                    Flag: {row['forecast_flag']}
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.write("")


def page_attendance_drivers(base_df: pd.DataFrame):
    st.title("Attendance Drivers")
    st.caption("Explore the strongest patterns tied to MLB attendance across weather, team performance, and scheduling.")

    filtered = filter_base_df(base_df)
    if filtered.empty:
        st.info("No rows match the selected filters.")
        return

# 🔍 DEBUG BUTTON (ADD THIS BLOCK)
    if st.button("Print Stats for Current Attendance Drivers View"):
        selected_team = st.session_state.get("Home Team", None)

        if selected_team is None or selected_team == "All Teams":
            print("Please select one team first, like Arizona Diamondbacks.")
        else:
            print_team_debug_stats(filtered, selected_team)

    team_avg = (
        filtered.groupby("home_team", as_index=False)["attendance"]
        .mean()
        .sort_values("attendance", ascending=False)
    )

    top_12_teams = team_avg.head(12)["home_team"].tolist()
    filtered_top = filtered[filtered["home_team"].isin(top_12_teams)].copy()

    # KPI row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Games Analyzed", fmt_int(len(filtered)))
    c2.metric("Avg Attendance", fmt_int(filtered["attendance"].mean()))
    c3.metric("Avg Temperature", fmt_num(filtered["temperature_f"].mean(), 1))
    c4.metric("Avg Home Win %", fmt_num(filtered["home_win_pct_10"].mean(), 2))

    # Add friendly weekend/weekday label
    filtered["game_type"] = filtered["weekend_flag"].map({True: "Weekend", False: "Weekday"})
    filtered_top["game_type"] = filtered_top["weekend_flag"].map({True: "Weekend", False: "Weekday"})

    # -------------------------
    # Key relationships
    # -------------------------
    st.markdown("### Key Relationships")
    left, right = st.columns(2)

    with left:
        fig_temp = px.scatter(
            filtered,
            x="temperature_f",
            y="attendance",
            color="game_type",
            title="Temperature vs Attendance",
            hover_data=["game_date", "home_team", "away_team"],
            trendline="ols",
        )
        fig_temp.update_traces(marker=dict(size=5), opacity=0.35)
        fig_temp.update_layout(
            height=390,
            margin=dict(l=20, r=20, t=50, b=20),
            legend_title="Game Type",
            xaxis_title="Temperature (°F)",
            yaxis_title="Attendance",
        )
        st.plotly_chart(fig_temp, use_container_width=True)

    with right:
        fig_win = px.scatter(
            filtered,
            x="home_win_pct_10",
            y="attendance",
            color="game_type",
            title="Home Rolling Win % vs Attendance",
            hover_data=["game_date", "home_team", "away_team"],
            trendline="ols",
        )
        fig_win.update_traces(marker=dict(size=5), opacity=0.35)
        fig_win.update_layout(
            height=390,
            margin=dict(l=20, r=20, t=50, b=20),
            legend_title="Game Type",
            xaxis_title="Home Rolling Win % (Last 10)",
            yaxis_title="Attendance",
        )
        st.plotly_chart(fig_win, use_container_width=True)

    # -------------------------
    # Team patterns
    # -------------------------
    st.markdown("### Team Patterns")
    c3, c4 = st.columns(2)

    with c3:
        fig_team = px.bar(
            team_avg.head(12),
            x="home_team",
            y="attendance",
            title="Top 12 Teams by Average Attendance",
        )
        fig_team.update_layout(
            height=380,
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis_title="",
            yaxis_title="Average Attendance",
            showlegend=False,
        )
        fig_team.update_xaxes(tickangle=40)
        st.plotly_chart(fig_team, use_container_width=True)

    with c4:
        heat_df = (
            filtered_top.groupby(["day_of_week", "home_team"], as_index=False)["attendance"]
            .mean()
        )

        if not heat_df.empty:
            pivot = heat_df.pivot(index="day_of_week", columns="home_team", values="attendance")
            day_order = [
                "Monday", "Tuesday", "Wednesday", "Thursday",
                "Friday", "Saturday", "Sunday"
            ]
            pivot = pivot.reindex([d for d in day_order if d in pivot.index])

            fig_heat = px.imshow(
                pivot,
                aspect="auto",
                labels=dict(color="Avg Attendance"),
                title="Day-of-Week Heatmap (Top 12 Teams)",
            )
            fig_heat.update_layout(
                height=380,
                margin=dict(l=20, r=20, t=50, b=20),
                xaxis_title="Home Team",
                yaxis_title="Day of Week",
            )
            st.plotly_chart(fig_heat, use_container_width=True)

    # -------------------------
    # Variability + correlation
    # -------------------------
    st.markdown("### Additional Insights")
    c5, c6 = st.columns(2)

    with c5:
        variability = (
            filtered_top.groupby("home_team", as_index=False)
            .agg(
                avg_attendance=("attendance", "mean"),
                std_attendance=("attendance", "std"),
            )
            .sort_values("avg_attendance", ascending=False)
        )

        fig_var = px.bar(
            variability,
            x="home_team",
            y="avg_attendance",
            error_y="std_attendance",
            title="Average Attendance with Variability (Top 12 Teams)",
        )
        fig_var.update_layout(
            height=420,
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis_title="",
            yaxis_title="Attendance",
            showlegend=False,
        )
        fig_var.update_xaxes(tickangle=40)
        st.plotly_chart(fig_var, use_container_width=True)

    with c6:
        corr_cols = [
            "attendance",
            "temperature_f",
            "precipitation_mm",
            "wind_kmh",
            "home_win_pct_10",
            "away_win_pct_10",
        ]
        corr_df = filtered[corr_cols].corr(numeric_only=True)

        fig_corr = px.imshow(
            corr_df,
            text_auto=".2f",
            aspect="auto",
            title="Feature Correlation Matrix",
        )
        fig_corr.update_layout(
            height=420,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        st.plotly_chart(fig_corr, use_container_width=True)

    # -------------------------
    # Takeaways
    # -------------------------
    st.markdown("### Quick Takeaways")

    top_team = team_avg.iloc[0]["home_team"] if not team_avg.empty else "N/A"
    top_team_att = team_avg.iloc[0]["attendance"] if not team_avg.empty else None

    weekday_avg = filtered.loc[filtered["weekend_flag"] == False, "attendance"].mean()
    weekend_avg = filtered.loc[filtered["weekend_flag"] == True, "attendance"].mean()
    weekend_lift = weekend_avg - weekday_avg if pd.notna(weekend_avg) and pd.notna(weekday_avg) else None

    attendance_corr = corr_df["attendance"].drop("attendance").sort_values(key=lambda s: s.abs(), ascending=False)
    strongest_driver = attendance_corr.index[0] if not attendance_corr.empty else "N/A"
    strongest_value = attendance_corr.iloc[0] if not attendance_corr.empty else None

    t1, t2, t3 = st.columns(3)
    t1.markdown(
        f"""
        <div class="metric-card">
            <b>Top Draw</b><br>
            {top_team}<br>
            Avg attendance: {fmt_int(top_team_att)}
        </div>
        """,
        unsafe_allow_html=True,
    )
    t2.markdown(
        f"""
        <div class="metric-card">
            <b>Weekend Effect</b><br>
            Weekday avg: {fmt_int(weekday_avg)}<br>
            Weekend avg: {fmt_int(weekend_avg)}<br>
            Lift: {fmt_int(weekend_lift)}
        </div>
        """,
        unsafe_allow_html=True,
    )
    t3.markdown(
        f"""
        <div class="metric-card">
            <b>Strongest Numeric Driver</b><br>
            {strongest_driver}<br>
            Correlation: {fmt_num(strongest_value, 2)}
        </div>
        """,
        unsafe_allow_html=True,
    )


def page_scenario_simulator(base_df: pd.DataFrame):
    st.title("Scenario Simulator")
    st.caption("Estimate likely attendance scenarios.")

    teams = load_team_list()
    if not teams:
        st.warning("No team records found.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        selected_team = st.selectbox("Home Team", teams, index=0)
    with col2:
        selected_day = st.selectbox(
            "Day of Week",
            ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
            index=5,
        )
    with col3:
        opponent_type = st.selectbox("Opponent Strength", ["Lower", "Average", "Stronger"], index=1)

    c4, c5, c6 = st.columns(3)
    with c4:
        temp_f = st.slider("Temperature (°F)", min_value=20, max_value=110, value=72)
    with c5:
        precipitation_mm = st.slider("Precipitation (mm)", min_value=0.0, max_value=15.0, value=0.0, step=0.5)
    with c6:
        home_win_pct_10 = st.slider("Home Rolling Win %", min_value=0.0, max_value=1.0, value=0.55, step=0.01)

    promo_on = st.toggle("Promotion / special draw", value=False)

    team_df = base_df[base_df["home_team"] == selected_team].copy()
    if team_df.empty:
        st.info("No historical rows found for that team.")
        return

    base_att = float(team_df["attendance"].mean())

    weekday_adjustment = {
        "Monday": -0.06,
        "Tuesday": -0.05,
        "Wednesday": -0.03,
        "Thursday": -0.01,
        "Friday": 0.05,
        "Saturday": 0.09,
        "Sunday": 0.04,
    }[selected_day]

    opponent_adjustment = {
        "Lower": -0.04,
        "Average": 0.0,
        "Stronger": 0.05,
    }[opponent_type]

    team_avg_temp = float(team_df["temperature_f"].dropna().mean()) if team_df["temperature_f"].notna().any() else 68.0
    team_avg_precip = float(team_df["precipitation_mm"].dropna().mean()) if team_df["precipitation_mm"].notna().any() else 0.0
    team_avg_win = float(team_df["home_win_pct_10"].dropna().mean()) if team_df["home_win_pct_10"].notna().any() else 0.5

    temp_effect = (temp_f - team_avg_temp) * 0.005
    precip_effect = -0.012 * max(0.0, precipitation_mm - team_avg_precip)
    win_effect = (home_win_pct_10 - team_avg_win) * 0.20
    promo_effect = 0.06 if promo_on else 0.0

    multiplier = 1 + weekday_adjustment + opponent_adjustment + temp_effect + precip_effect + win_effect + promo_effect
    pred = max(0, base_att * multiplier)

    historical_mae = float((team_df["attendance"] - team_df["attendance"].mean()).abs().mean())
    lo95 = max(0, pred - 1.96 * historical_mae)
    hi95 = pred + 1.96 * historical_mae

    m1, m2, m3 = st.columns(3)
    m1.metric("Projected Attendance", fmt_int(pred))
    m2.metric("Low Expected Range", fmt_int(lo95))
    m3.metric("High Expected Range", fmt_int(hi95))

    st.markdown("### Scenario Explanation")
    st.write(
        f"""
        This scenario starts from **{selected_team}'s historical average home attendance** and adjusts it
        for day of week, recent team form, weather, and whether a promotion is running.
        It is designed as a presentation-friendly simulator, not a replacement for your saved production forecasts.
        """
    )

    contrib = pd.DataFrame(
        {
            "Factor": [
                "Base Team Level",
                "Day of Week",
                "Opponent Strength",
                "Temperature",
                "Precipitation",
                "Recent Form",
                "Promotion",
            ],
            "Effect": [
                0.0,
                weekday_adjustment,
                opponent_adjustment,
                temp_effect,
                precip_effect,
                win_effect,
                promo_effect,
            ],
        }
    )
    fig = px.bar(contrib, x="Factor", y="Effect", title="Scenario Effect Breakdown")
    fig.update_layout(height=420)
    st.plotly_chart(fig, use_container_width=True)

    hist_view = team_df[["game_date", "attendance"]].sort_values("game_date").copy()
    hist_view["scenario_line"] = pred
    fig_hist = go.Figure()
    fig_hist.add_trace(go.Scatter(x=hist_view["game_date"], y=hist_view["attendance"], mode="lines+markers", name="Historical"))
    fig_hist.add_trace(go.Scatter(x=hist_view["game_date"], y=hist_view["scenario_line"], mode="lines", name="Scenario Estimate"))
    fig_hist.update_layout(title=f"Historical Attendance vs Scenario for {selected_team}", height=420)
    st.plotly_chart(fig_hist, use_container_width=True)


# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    style_app()

    base_df = load_base_data()
    forecast_df = load_forecasts()

    with st.sidebar:
        st.title("Navigation")
        page = st.radio(
            "Choose a page",
            [
                "Executive Forecast View",
                "Attendance Drivers",
                "Scenario Simulator",
            ],
            index=0,
        )
        st.markdown("---")
        st.caption("Forecast demand, identify risk, and explore game-level attendance patterns.")

    if page == "Executive Forecast View":
        page_executive_forecast(base_df, forecast_df)
    elif page == "Attendance Drivers":
        page_attendance_drivers(base_df)
    else:
        page_scenario_simulator(base_df)


if __name__ == "__main__":
    main()
