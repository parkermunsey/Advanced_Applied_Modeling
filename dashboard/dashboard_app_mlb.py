
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


def style_app():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 1rem;
        }
        .metric-card {
            background-color: #f7f9fc;
            padding: 1rem 1.1rem;
            border-radius: 14px;
            border: 1px solid #e6ebf2;
        }
        .section-label {
            font-size: 1.1rem;
            font-weight: 600;
            margin-top: 0.5rem;
            margin-bottom: 0.4rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def filter_base_df(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### Filters")

    teams = ["All Teams"] + load_team_list()
    selected_team = st.sidebar.selectbox("Home Team", teams, index=0)

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
    filtered = add_risk_flags(filtered)

    if filtered.empty:
        st.info("No forecast rows match the selected filters.")
        return

    latest_run = filtered["run_ts_utc"].max()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Games in View", fmt_int(len(filtered)))
    c2.metric("Avg Predicted Attendance", fmt_int(filtered["pred_attendance"].mean()))
    c3.metric("Highest Predicted Game", fmt_int(filtered["pred_attendance"].max()))
    c4.metric("Avg Expected Range Width", fmt_int((filtered["hi95"] - filtered["lo95"]).mean()))

    st.markdown(f"**Latest forecast run:** {latest_run.strftime('%Y-%m-%d %H:%M UTC') if pd.notna(latest_run) else 'N/A'}")

    chart_df = filtered.sort_values("game_date").copy()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=chart_df["game_date"],
            y=chart_df["hi95"],
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=chart_df["game_date"],
            y=chart_df["lo95"],
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            name="Expected Range",
            hovertemplate="Low: %{y:,.0f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=chart_df["game_date"],
            y=chart_df["pred_attendance"],
            mode="lines+markers",
            name="Predicted Attendance",
            hovertemplate="%{x|%Y-%m-%d}<br>Predicted: %{y:,.0f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Predicted Attendance with Expected Range",
        xaxis_title="Game Date",
        yaxis_title="Attendance",
        height=430,
        margin=dict(l=20, r=20, t=60, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns([1.5, 1])
    with left:
        display_cols = [
            "game_date",
            "home_team",
            "away_team",
            "venue_name",
            "pred_attendance",
            "lo95",
            "hi95",
            "temperature_f",
            "precipitation_mm",
            "forecast_flag",
        ]
        display_df = chart_df[display_cols].copy()
        display_df["game_date"] = display_df["game_date"].dt.strftime("%Y-%m-%d")
        st.markdown("### Forecast Table")
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
                    "temperature_f": "Temp (F)",
                    "precipitation_mm": "Precip (mm)",
                    "forecast_flag": "Flag",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    with right:
        st.markdown("### Forecast Flags")
        flag_counts = chart_df["forecast_flag"].value_counts().reset_index()
        flag_counts.columns = ["flag", "games"]
        fig_flags = px.bar(flag_counts, x="flag", y="games", title="Games by Flag")
        fig_flags.update_layout(height=320, margin=dict(l=20, r=20, t=60, b=20))
        st.plotly_chart(fig_flags, use_container_width=True)

        risk_games = chart_df[chart_df["forecast_flag"] != "Monitor"].copy()
        st.markdown("### Priority Games")
        if risk_games.empty:
            st.success("No high-risk or high-crowd games in the current filter view.")
        else:
            for _, row in risk_games.head(6).iterrows():
                st.markdown(
                    f"""
                    <div class="metric-card">
                        <b>{row['home_team']} vs {row['away_team']}</b><br>
                        {row['game_date'].strftime('%Y-%m-%d')}<br>
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
    st.caption("Explore the structural and environmental drivers tied to MLB attendance.")

    filtered = filter_base_df(base_df)
    if filtered.empty:
        st.info("No rows match the selected filters.")
        return

    c1, c2 = st.columns(2)

    with c1:
        fig_temp = px.scatter(
            filtered,
            x="temperature_f",
            y="attendance",
            color="home_team",
            trendline="ols",
            title="Temperature vs Attendance",
            hover_data=["game_date", "away_team", "venue_name"],
        )
        fig_temp.update_layout(height=420, showlegend=False)
        st.plotly_chart(fig_temp, use_container_width=True)

    with c2:
        fig_win = px.scatter(
            filtered,
            x="home_win_pct_10",
            y="attendance",
            color="weekend_flag",
            title="Home Rolling Win % vs Attendance",
            hover_data=["game_date", "home_team", "away_team"],
        )
        fig_win.update_layout(height=420)
        st.plotly_chart(fig_win, use_container_width=True)

    c3, c4 = st.columns(2)

    with c3:
        team_avg = (
            filtered.groupby("home_team", as_index=False)["attendance"]
            .mean()
            .sort_values("attendance", ascending=False)
        )
        fig_team = px.bar(
            team_avg,
            x="home_team",
            y="attendance",
            title="Average Attendance by Home Team",
        )
        fig_team.update_layout(height=420, xaxis_title="", yaxis_title="Average Attendance")
        st.plotly_chart(fig_team, use_container_width=True)

    with c4:
        heat_df = (
            filtered.groupby(["day_of_week", "home_team"], as_index=False)["attendance"]
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
                title="Day of Week Heatmap by Home Team",
            )
            fig_heat.update_layout(height=420)
            st.plotly_chart(fig_heat, use_container_width=True)

    st.markdown("### Distribution by Home Team")
    fig_box = px.box(
        filtered,
        x="home_team",
        y="attendance",
        points="outliers",
        title="Attendance Distribution by Team",
    )
    fig_box.update_layout(height=460, xaxis_title="", yaxis_title="Attendance")
    st.plotly_chart(fig_box, use_container_width=True)

    corr_cols = [
        "attendance",
        "temperature_f",
        "precipitation_mm",
        "wind_kmh",
        "home_win_pct_10",
        "away_win_pct_10",
    ]
    corr_df = filtered[corr_cols].corr(numeric_only=True)
    st.markdown("### Correlation View")
    fig_corr = px.imshow(
        corr_df,
        text_auto=".2f",
        aspect="auto",
        title="Feature Correlation Matrix",
    )
    fig_corr.update_layout(height=500)
    st.plotly_chart(fig_corr, use_container_width=True)


def page_scenario_simulator(base_df: pd.DataFrame):
    st.title("Scenario Simulator")
    st.caption("Use historical averages plus your model logic to estimate likely attendance scenarios.")

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
        st.caption("Built for the MLB attendance forecasting capstone project.")

    if page == "Executive Forecast View":
        page_executive_forecast(base_df, forecast_df)
    elif page == "Attendance Drivers":
        page_attendance_drivers(base_df)
    else:
        page_scenario_simulator(base_df)


if __name__ == "__main__":
    main()
