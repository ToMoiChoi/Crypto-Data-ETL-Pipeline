# 🧩 Crypto Data ETL Pipeline

## 📘 Overview

This project demonstrates a complete **ETL (Extract, Transform, Load)**
pipeline built in **Python** for cryptocurrency data. The goal is to
extract real-time market data from the **Binance API**, transform it
into a clean and structured dataset, and load it into a database
(**SQLite** or **BigQuery**) for analytics, automation, and reporting.

This project showcases essential **Data Engineering skills**, including:

-   API integration
-   Data transformation using pandas
-   Database loading
-   Workflow orchestration using **Apache Airflow**
-   Cloud integration (BigQuery)

## ⚙️ Tech Stack

  Category                     Tools / Technologies
  ---------------------------- ----------------------------------
  **Programming Language**     Python 3.x
  **Libraries**                pandas, numpy, requests, sqlite3
  **Workflow Orchestration**   Apache Airflow
  **API Source**               Binance REST API
  **Local Database**           SQLite
  **Cloud Data Warehouse**     Google BigQuery
  **Containerization**         Docker / Docker Compose
  **Visualization**            Power BI / matplotlib (optional)
  **Environment**              Jupyter Notebook

## 🧠 ETL Workflow

### 1. **Extract**

Retrieve BTC/USDT daily market data from Binance API and convert JSON
into a pandas DataFrame.

``` python
url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=30"
response = requests.get(url)
```

### 2. **Transform**

Convert timestamps, clean data, add calculated metrics.

``` python
df['Date'] = pd.to_datetime(df['Open_time'], unit='ms')
df['Daily Change %'] = df['Close'].pct_change() * 100
```

### 3. **Load**

Store data into SQLite or load into BigQuery.

``` python
df.to_sql('CryptoData', con=sql_connection, if_exists='replace', index=False)
```

## 🧩 Project Structure

    crypto_etl_pipeline.ipynb     # Notebook version of the ETL pipeline
    crypto_data.csv               # Cleaned crypto dataset
    CryptoData.db                 # SQLite database
    README.md

    airflow/
    │── dags/
    │   └── crypto_etl_dag.py     # Airflow DAG
    │── logs/                     # Airflow logs
    │── plugins/                  # Optional custom operators
    │── keys/
    │   └── gcp_key.json          # GCP credentials (gitignored)
    │── docker-compose.yml        # Airflow deployment

# 🔄 Apache Airflow Integration (Workflow Orchestration)

This project includes an **Apache Airflow DAG** that automates the ETL
pipeline.\
Airflow provides:

-   Scheduling
-   Dependency management
-   Logging & monitoring
-   Retry & failure handling

### ETL Flow inside Airflow

    Extract → Transform → Load

### Airflow DAG: `crypto_etl_dag.py`

The DAG orchestrates all 3 tasks:

``` python
extract_task >> transform_task >> load_task
```

## 👤 Author

**Tô Quang Việt**\
📍 Hanoi, Vietnam\
📧 viettoquang2003@gmail.com\
🔗 https://www.linkedin.com/in/vtqa6\
💻 https://github.com/viettoqang
