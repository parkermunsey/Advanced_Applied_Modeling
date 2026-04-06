<h1 align="center">MLB Attendance Forecasting System</h1>
<h3 align="center">Predicting Game-Level Demand with Data</h3>

<p align="center">
  <em>
    Project by Parker Munsey<br>
    University of Montana MSBA
    Advanced Applied Modeling
  </em>
</p>

---

## Project Overview

Major League Baseball attendance is influenced by a combination of team performance, scheduling factors, and external conditions such as weather. However, most organizations rely on historical reporting rather than forward-looking insights.

This project builds an end-to-end data pipeline and dashboard that forecasts MLB game attendance and translates predictions into actionable insights.

The system integrates multiple data sources, applies predictive modeling, and presents results through an interactive dashboard designed for decision-making.

---

## Objective

The goal of this project is to move beyond reactive reporting and create a system that can:

- Predict game-level attendance  
- Quantify uncertainty using prediction intervals  
- Identify high-demand and at-risk games  
- Explain key drivers of attendance  
- Support operational decisions (staffing, concessions, promotions)  

---

## Repository Structure

- **scripts/ingestion/**  
  Collects MLB game data and weather data from external APIs  

- **scripts/feature_engineering/**  
  Builds model-ready datasets from raw data  

- **scripts/modeling/**  
  Trains and evaluates machine learning models  

- **scripts/forecasting/**  
  Generates predictions and prediction intervals  

- **scripts/database/**  
  Database setup, schema creation, and SQL queries  

- **dashboard/**  
  Streamlit application for visualization and analysis  

- **docs/**  
  Project documentation, diagrams, and images  

- **README.md**  
  Project overview and setup instructions  

---

## Why This Project Matters

Sports organizations make critical operational decisions based on expected attendance, including:

- Staffing levels  
- Concessions inventory  
- Marketing and promotions  

Without predictive insight, these decisions are often reactive or inefficient.

This project provides a framework for:

- Proactive planning  
- Risk identification  
- Data-driven decision-making  

---

## Data Sources

The system integrates multiple data sources:

- **MLB Stats API**  
  Game schedules, scores, attendance, teams, and venues  

- **Open-Meteo API**  
  Historical weather data (temperature, precipitation, wind)  

---

## System Architecture

The project follows a structured pipeline:

**Ingestion → Feature Engineering → Modeling → Forecasting → Dashboard**

### Database (PostgreSQL: `sports`)

**Dimension Tables**
- dim_team
- dim_date
- dim_venue

**Fact Tables**
- fact_game
- fact_weather
- fact_team_form

**Modeling Tables**
- ml_features_attendance
- fact_attendance_forecast

---

## Data Pipeline

### 1. Data Ingestion
- Pulls game-level data from MLB API  
- Pulls hourly weather data from Open-Meteo  
- Stores normalized data in PostgreSQL  

### 2. Feature Engineering
Creates model-ready features including:

- Temperature, precipitation, wind  
- Day of week, weekend flag, month  
- Rolling team performance (last 5, 10, 20 games)  
- Venue and team-level characteristics  

---

## Modeling

Two models were developed and evaluated:

- Ridge Regression (Final Model)
- Random Forest (Comparison Model)

### Evaluation Metrics

- MAE (Mean Absolute Error)  
- RMSE (Root Mean Squared Error)  
- R² (Model Fit)  
- Residual Analysis  

### Result

Ridge regression was selected as the final model due to:

- Better generalization  
- More stable predictions  
- Stronger performance on unseen data  

---

## Forecasting

The system generates:

- Predicted attendance  
- 95% prediction intervals (uncertainty bounds)  

Results are stored in:

- fact_attendance_forecast  

---

## Classification Layer (Business Translation)

To make predictions actionable, forecasts are categorized into:

- Low Demand  
- Normal  
- High Demand  

This allows non-technical users to quickly interpret results.

---

## Dashboard (Streamlit)

The dashboard serves as the presentation layer and includes three main views:

### 1. Executive Forecast View
- Predicted attendance over time  
- Confidence intervals  
- Game-level forecast table  

Forecast Flags:
- High Crowd  
- Weather Risk  
- Monitor  

---

### 2. Attendance Drivers

Explores relationships between attendance and:

- Weather  
- Team performance  
- Day of week  

Includes:

- Scatter plots  
- Heatmaps  
- Boxplots  
- Correlation matrix  

---

### 3. Scenario Simulator

Interactive “what-if” tool that allows users to adjust:

- Team  
- Opponent strength  
- Weather conditions  
- Day of week  
- Recent performance  

Outputs projected attendance and expected range.

---

## How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run ingestion pipeline
```bash
python scripts/ingestion/load_mlb_games_weather_to_postgres.py
```

### 3. Build features
```bash
python scripts/modeling/build_features.py
```

### 4. Run forecasting
```bash
python scripts/forecasting/run_forecast.py
```

### 5. Launch dashboard
```bash
streamlit run dashboard/app.py
```

---

## Key Insights

- Attendance is driven more by structural factors (team, venue, schedule) than weather alone  
- Ridge regression outperformed random forest in both regression and classification tasks  
- The model is conservative when predicting high-demand games  
- Weekend games and opponent quality significantly impact attendance  

---

## Current Limitations

- Primarily based on historical data (limited forward schedule integration)  
- Limited number of seasons included  
- Simplified opponent strength metric  
- Missing features such as promotions, holidays, and rivalry effects  
- Some duplicate forecast rows require cleanup  

---

## Next Steps

- Integrate future MLB schedules for forward-looking forecasts  
- Add additional seasons (2025+) for improved model stability  
- Improve opponent strength using advanced metrics  
- Incorporate promotional and event-based features  
- Enhance dashboard UI and decision recommendations  

---

## Summary

This project demonstrates a complete data pipeline from raw ingestion to decision-ready insights.

It combines:

- Data engineering  
- Statistical modeling  
- Machine learning  
- Dashboard development  

The result is a system that not only predicts MLB attendance but also makes those predictions interpretable, actionable, and valuable for real-world decision-making.

---

## Author

Parker Munsey  
University of Montana  
Master of Science in Business Analytics (MSBA)
