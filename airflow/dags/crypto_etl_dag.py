from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
import logging

import requests
from requests.adapters import HTTPAdapter, Retry

import pandas as pd
from google.cloud import bigquery
import os


BTC_SYMBOL = "BTCUSDT"
TMP_FILE = "/opt/airflow/dags/btcusdt_daily.csv"


# ---------------------------------------------------
# 1. EXTRACT – API call with retry + timeout
# ---------------------------------------------------
def extract_data(ti):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": BTC_SYMBOL,
        "interval": "1d",
        "limit": 30
    }

    # Create session with retry strategy
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Request
    response = session.get(url, params=params, timeout=10)
    response.raise_for_status()

    ti.xcom_push(key="api_response", value=response.json())
    logging.info("Extracted raw BTCUSDT data successfully.")


# ---------------------------------------------------
# 2. CONVERT raw JSON → DataFrame
# ---------------------------------------------------
def convert_data(ti):
    data = ti.xcom_pull(key="api_response")

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])

    ti.xcom_push(key="raw_df", value=df.to_json())
    logging.info(f"Converted raw JSON → DataFrame with {len(df)} rows.")


# ---------------------------------------------------
# 3. TRANSFORM
# ---------------------------------------------------
def transform_data(ti):
    raw_json = ti.xcom_pull(key="raw_df")
    df = pd.read_json(raw_json)

    # Convert numeric
    num_cols = ["open", "high", "low", "close", "volume"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    # Convert timestamp
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    # Calculate metrics
    df["daily_change"] = df["close"] - df["open"]
    df["daily_return_pct"] = (df["daily_change"] / df["open"]) * 100
    df["daily_return_pct"] = df["daily_return_pct"].round(2)
    # Validate
    if df.isnull().sum().sum() > 0:
        raise ValueError("Data contains NaN values after transform!")

    ti.xcom_push(key="clean_df", value=df.to_json())
    logging.info("Transformed BTCUSDT data successfully.")


# ---------------------------------------------------
# 4. LOAD TO CSV (local temp)
# ---------------------------------------------------
def load_to_csv(ti):
    clean_json = ti.xcom_pull(key="clean_df")
    df = pd.read_json(clean_json)

    df.to_csv(TMP_FILE, index=False)

    ti.xcom_push(key="csv_path", value=TMP_FILE)
    logging.info(f"Saved BTCUSDT data to {TMP_FILE}")


# ---------------------------------------------------
# 5. LOAD TO BIGQUERY
# ---------------------------------------------------
def load_bigquery(ti):
    clean_json = ti.xcom_pull(key="clean_df")
    df = pd.read_json(clean_json)


    client = bigquery.Client()
    table_id = "japanese-food-shop-analysis.crypto_dataset.btcusdt_daily"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
        location="US"
    )
    
    job.result()
    logging.info(f"Loaded {len(df)} rows to partitioned BigQuery table.")


# ---------------------------------------------------
# DAG CONFIG
# ---------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="BTCUSDT_ETL_Production_v01",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["crypto", "btc", "etl", "bigquery"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    convert = PythonOperator(
        task_id="convert_data",
        python_callable=convert_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    save_csv = PythonOperator(
        task_id="load_to_csv",
        python_callable=load_to_csv
    )

    load_bq = PythonOperator(
        task_id="load_bigquery",
        python_callable=load_bigquery
    )

    # Task pipeline
    extract >> convert >> transform >> [save_csv, load_bq]
