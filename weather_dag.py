"""
Weather ETL pipeline (Apache Airflow 3).

Extract   : call the OpenWeatherMap "current weather" API for a city.
Transform : flatten the JSON response and convert temperatures to Fahrenheit
            and timestamps to the city's local time.
Load      : upload the result as a timestamped CSV file to an S3 bucket.

Required Airflow setup (no secrets live in this file):
  * Connection `weathermap_api`      -> HTTP, host: https://api.openweathermap.org
  * Connection `aws_default`         -> AWS; leave credentials empty to use the
                                        EC2 instance's IAM role
  * Variable   `openweather_api_key` -> your OpenWeatherMap API key
  * Variable   `weather_s3_bucket`   -> name of the target S3 bucket
Requires the package: apache-airflow-providers-amazon
"""

import json
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

CITY = "Paris"
HTTP_CONN_ID = "weathermap_api"
AWS_CONN_ID = "aws_default"
S3_PREFIX = "weather"
WEATHER_ENDPOINT = (
    "/data/2.5/weather?q={{ params.city }}"
    "&appid={{ var.value.openweather_api_key }}"
)


# ---------------------------------------------------------------- helpers
def kelvin_to_fahrenheit(temp_kelvin: float) -> float:
    """Convert a temperature from Kelvin to Fahrenheit."""
    return round((temp_kelvin - 273.15) * 9 / 5 + 32, 2)


def to_local_time(unix_ts: int, utc_offset_seconds: int) -> datetime:
    """Convert a UTC Unix timestamp to the city's local (naive) datetime."""
    return datetime.fromtimestamp(unix_ts + utc_offset_seconds, tz=timezone.utc).replace(tzinfo=None)


def transform_weather(data: dict) -> pd.DataFrame:
    """Flatten the raw API response into a one-row DataFrame."""
    main = data["main"]
    offset = data["timezone"]

    record = {
        "city": data["name"],
        "description": data["weather"][0]["description"],
        "temperature_f": kelvin_to_fahrenheit(main["temp"]),
        "feels_like_f": kelvin_to_fahrenheit(main["feels_like"]),
        "min_temp_f": kelvin_to_fahrenheit(main["temp_min"]),
        "max_temp_f": kelvin_to_fahrenheit(main["temp_max"]),
        "pressure_hpa": main["pressure"],
        "humidity_pct": main["humidity"],
        "wind_speed_ms": data["wind"]["speed"],
        "time_of_record_local": to_local_time(data["dt"], offset),
        "sunrise_local": to_local_time(data["sys"]["sunrise"], offset),
        "sunset_local": to_local_time(data["sys"]["sunset"], offset),
    }
    return pd.DataFrame([record])


# ------------------------------------------------------------------ task
def transform_load_data(ti) -> str:
    """Pull the API response from XCom, transform it and upload it to S3 as CSV."""
    raw = ti.xcom_pull(task_ids="extract_weather_data")
    df = transform_weather(raw)

    bucket = Variable.get("weather_s3_bucket")
    city_slug = df.loc[0, "city"].lower().replace(" ", "_")
    now = datetime.now(timezone.utc)
    s3_key = (
        f"{S3_PREFIX}/{city_slug}/{now:%Y/%m/%d}/"
        f"current_weather_{city_slug}_{now:%Y%m%d_%H%M%S}.csv"
    )

    S3Hook(aws_conn_id=AWS_CONN_ID).load_string(
        string_data=df.to_csv(index=False),
        key=s3_key,
        bucket_name=bucket,
        replace=True,
    )
    return f"s3://{bucket}/{s3_key}"


# ------------------------------------------------------------------- DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_dag",
    description="Daily ETL of current weather data from OpenWeatherMap",
    default_args=default_args,
    start_date=datetime(2023, 1, 8),
    schedule="@daily",
    catchup=False,
    params={"city": CITY},
    tags=["etl", "weather"],
) as dag:

    is_weather_api_ready = HttpSensor(
        task_id="is_weather_api_ready",
        http_conn_id=HTTP_CONN_ID,
        endpoint=WEATHER_ENDPOINT,
        poke_interval=30,
        timeout=300,
    )

    extract_weather_data = HttpOperator(
        task_id="extract_weather_data",
        http_conn_id=HTTP_CONN_ID,
        endpoint=WEATHER_ENDPOINT,
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=False,  # avoid printing the full payload in logs
    )

    transform_load_weather_data = PythonOperator(
        task_id="transform_load_weather_data",
        python_callable=transform_load_data,
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
