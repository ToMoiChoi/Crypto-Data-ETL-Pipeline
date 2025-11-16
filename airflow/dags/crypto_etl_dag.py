from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import requests
import pandas as pd
from google.cloud import bigquery


# -------------------------
# 1. EXTRACT ---------------
# -------------------------
def extract_data(ti):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "limit": 30
    }
    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception("API request failed")

    data = response.json()
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])

    ti.xcom_push(key="raw_data", value=df.to_json())
    print(df.head())


# -------------------------
# 2. TRANSFORM ------------
# -------------------------
def transform_data(ti):
    raw_json = ti.xcom_pull(key="raw_data")
    df = pd.read_json(raw_json)

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(float)

    df["daily_change"] = df["close"] - df["open"]
    df["daily_return_pct"] = (df["daily_change"] / df["open"]) * 100

    ti.xcom_push(key="clean_data", value=df.to_json())
    print(df.head())


# -------------------------
# 3. LOAD → BIGQUERY ------
# -------------------------
def load_bigquery(ti):
    clean_json = ti.xcom_pull(key="clean_data")
    df = pd.read_json(clean_json)

    client = bigquery.Client()

    table_id = "japanese-food-shop-analysis.crypto_dataset.btcusdt_daily"

    job = client.load_table_from_dataframe(
        df,
        table_id,
        location="US"   
    )

    job.result()

    print(f"Loaded {len(df)} rows to BigQuery table: {table_id}")


# =====================================================
# DAG CONFIG
# =====================================================
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)   # FIXED !!!
}

with DAG(
    dag_id="Crypto_etl_pipeline_v01",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["crypto", "etl", "bigquery", "binance"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id="load_bigquery",
        python_callable=load_bigquery
    )

    extract >> transform >> load
