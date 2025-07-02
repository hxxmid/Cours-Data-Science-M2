from airflow.decorators import dag, task
from datetime import datetime
import pendulum
import requests
import pandas as pd
import os

# 📍 Configuration
CITIES = {
    "Paris": {"latitude": 48.85, "longitude": 2.35},
    "Londres": {"latitude": 51.51, "longitude": -0.13},
    "Berlin": {"latitude": 52.52, "longitude": 13.40}
}

CSV_PATH = "/opt/airflow/data/weather_data.csv"


@dag(
    dag_id="weather_etl",
    start_date=datetime(2025, 7, 2, tzinfo=pendulum.timezone("UTC")),
    schedule="0 8 * * *",  # Tous les jours à 8h
    catchup=False,
    tags=["weather", "ETL", "open-meteo"]
)
def weather_etl():

    @task()
    def extract():
        records = []
        for city, coord in CITIES.items():
            url = f"https://api.open-meteo.com/v1/forecast?latitude={coord['latitude']}&longitude={coord['longitude']}&current_weather=true"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json().get("current_weather", {})
                records.append({
                    "city": city,
                    "timestamp": datetime.utcnow().isoformat(),
                    "temperature": data.get("temperature"),
                    "windspeed": data.get("windspeed"),
                    "weathercode": data.get("weathercode")
                })
        return records

    @task()
    def transform(data):
        df = pd.DataFrame(data)
        return df.to_dict(orient="records")  # Serializable for XCom

    @task()
    def load(data):
        df_new = pd.DataFrame(data)
        if os.path.exists(CSV_PATH):
            df_existing = pd.read_csv(CSV_PATH)
            df_combined = pd.concat([df_existing, df_new])
            df_combined.drop_duplicates(subset=["city", "timestamp"], inplace=True)
        else:
            df_combined = df_new

        df_combined.to_csv(CSV_PATH, index=False)

    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)


dag_instance = weather_etl()
