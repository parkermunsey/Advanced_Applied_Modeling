<h1 align="center">MLB Attendance Analysis</h1>

<p align="center">
  <img src="media/mlb_logo1.png" width="140"/>
</p>

<h3 align="center">Forecasting Game-Level Demand with Data</h3>

<p align="center">
  <em>
    Parker Munsey<br>
    University of Montana MSBA<br>
    Advanced Applied Modeling
  </em>
</p>

---

## Project Overview

MLB attendance is influenced by a mix of performance, scheduling, and external factors like weather. However, most organizations rely on historical reporting rather than forward-looking insights.

This project builds an end-to-end data pipeline and dashboard that forecasts MLB game attendance and translates predictions into actionable insights.

---

## Objective

This system moves beyond reactive reporting by enabling:

- Game-level attendance forecasting  
- Prediction intervals (uncertainty)  
- Identification of high-demand and at-risk games  
- Understanding of attendance drivers  
- Data-driven operational decisions  

---

## Why This Project Matters

Teams make decisions on:

- Staffing  
- Concessions  
- Promotions  

This project enables proactive planning instead of reactive guessing.

---

## System Architecture

Ingestion → Database → Features → Model → Forecasts → Dashboard

### Database (PostgreSQL)

Dimension Tables:
- dim_team  
- dim_date  
- dim_venue  

Fact Tables:
- fact_game  
- fact_weather  

Modeling Tables:
- ml_features_attendance  
- fact_attendance_forecast  

---

## Data Sources

- MLB Stats API → games, attendance, teams  
- Open-Meteo API → weather  

---

## Repository Structure

    project_root/
    │
    ├── scripts/
    │   ├── load_mlb_games_weather_to_postgres.py
    │   ├── build_features.py
    │   ├── export_features_for_model.py
    │   ├── train_model.py
    │   ├── evaluate_model.py
    │   ├── write_forecasts_to_postgres.py
    │
    ├── data/                  (parquet train/valid/test)
    ├── models/                (saved models)
    ├── outputs/               (predictions)
    │
    ├── dashboard/
    │   └── dashboard_app_mlb.py
    │
    ├── media/
    │   └── mlb_logo1.png
    │
    ├── requirements.txt
    ├── .env.example
    └── README.md

---

## How to Run (Full Setup)

1. Clone repository  
   git clone <your-repo-url>  
   cd <repo-name>  

2. Create virtual environment  
   python -m venv venv  

   Activate:  
   Windows → venv\Scripts\activate  
   Mac/Linux → source venv/bin/activate  

3. Install dependencies  
   pip install -r requirements.txt  

4. Set up PostgreSQL  
   CREATE DATABASE sports;  

5. Create .env file in root  
   PGHOST=localhost  
   PGDATABASE=sports  
   PGUSER=postgres  
   PGPASSWORD=your_password_here  
   PGPORT=5432  

6. Run pipeline (in this order)  
   python scripts/load_mlb_games_weather_to_postgres.py --start-date 2022-01-01 --end-date 2024-12-31  
   python scripts/build_features.py  
   python scripts/export_features_for_model.py  
   python scripts/train_model.py  
   python scripts/evaluate_model.py  
   python scripts/write_forecasts_to_postgres.py  

7. Launch dashboard  
   streamlit run dashboard/dashboard_app_mlb.py  

---

## Dashboard Overview

Executive Forecast View:
- Predicted attendance  
- Confidence intervals  
- Risk flags (High Crowd, Weather Risk)  

Attendance Drivers:
- Weather vs attendance  
- Team performance vs attendance  
- Correlation analysis  

Scenario Simulator:
- Adjust inputs  
- View projected attendance instantly  

---

## Modeling

Models tested:
- Ridge Regression  
- Random Forest  

Final selection based on:
- Generalization performance  
- Stability  
- Error metrics (MAE, RMSE, R²)  

---

## Forecasting

Outputs:
- Predicted attendance  
- 95% prediction intervals  

Stored in:
fact_attendance_forecast  

---

## Key Insights

- Weekend games significantly increase attendance  
- Team performance impacts demand more than weather  
- Weather matters primarily in extreme conditions  
- Model is conservative for high-demand games  

---

## Limitations

- Limited historical seasons  
- No promotional/event data  
- Simplified opponent strength  
- Requires local PostgreSQL setup  

---

## Future Improvements

- Add ticket pricing and promotions  
- Improve opponent strength metrics  
- Automate pipeline scheduling  
- Deploy to cloud environment  

---

## Summary

This project demonstrates a complete pipeline:

- Data Engineering  
- Machine Learning  
- Forecasting  
- Dashboarding  

It transforms raw MLB data into decision-ready insights.

---

## Author

Parker Munsey  
University of Montana  
Master of Science in Business Analytics (MSBA)
