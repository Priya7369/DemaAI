import os
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine

DATA_PATH = '/opt/airflow/dags/data/'

def download_orders():
    url = "URL_TO_ORDERS_DATASET"
    file_path = os.path.join(DATA_PATH, 'orders.csv')
    if not os.path.exists(file_path):
        response = requests.get(url)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        logging.info("Orders data downloaded.")
    else:
        logging.info("Orders file already exists.")

def download_inventory():
    url = "URL_TO_INVENTORY_DATASET"
    file_path = os.path.join(DATA_PATH, 'inventory.csv')
    if not os.path.exists(file_path):
        response = requests.get(url)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        logging.info("Inventory data downloaded.")
    else:
        logging.info("Inventory file already exists.")

def ingest_data():
    engine = create_engine('postgresql+psycopg2://dema_user:dema_password@postgres:5432/dema_db')
    orders_df = pd.read_csv(os.path.join(DATA_PATH, 'orders.csv'))
    inventory_df = pd.read_csv(os.path.join(DATA_PATH, 'inventory.csv'))
    orders_df.to_sql('orders', engine, if_exists='replace', index=False)
    inventory_df.to_sql('inventory', engine, if_exists='replace', index=False)
    logging.info("Data ingested into PostgreSQL.")

def transform_data():
    engine = create_engine('postgresql+psycopg2://dema_user:dema_password@postgres:5432/dema_db')
    orders_df = pd.read_sql('orders', engine)
    inventory_df = pd.read_sql('inventory', engine)
    merged_df = pd.merge(orders_df, inventory_df, on='product_id')
    merged_df.to_sql('transformed_data', engine, if_exists='replace', index=False)
    logging.info("Data transformed and merged.")

def validate_data():
    engine = create_engine('postgresql+psycopg2://dema_user:dema_password@postgres:5432/dema_db')
    transformed_df = pd.read_sql('transformed_data', engine)

    # Check for missing values
    if transformed_df.isnull().values.any():
        raise ValueError("Data validation failed: Missing values found")

    # Check for unique orderID and productID
    if transformed_df.duplicated(subset=['order_id', 'product_id']).any():
        raise ValueError("Data validation failed: Duplicate order_id and product_id found")

    # Check for no future dates in order_date
    if (transformed_df['order_date'] > pd.Timestamp.now()).any():
        raise ValueError("Data validation failed: Future dates found in order_date")

    logging.info("Data validation passed.")

def persist_data():
    engine = create_engine('postgresql+psycopg2://dema_user:dema_password@postgres:5432/dema_db')
    transformed_df = pd.read_sql('transformed_data', engine)
    transformed_df.to_sql('final_data', engine, if_exists='replace', index=False)
    logging.info("Final data persisted.")

def generate_reports():
    engine = create_engine('postgresql+psycopg2://dema_user:dema_password@postgres:5432/dema_db')
    final_df = pd.read_sql('final_data', engine)

    # Example report: total sales per product
    report1 = final_df.groupby('product_id')['quantity'].sum()
    logging.info("Report generated: Total sales per product.")
    print(report1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('ecommerce_data_pipeline',
         default_args=default_args,
         schedule_interval='@every_2_hours',
         catchup=False) as dag:

    download_orders_task = PythonOperator(
        task_id='download_orders',
        python_callable=download_orders
    )

    download_inventory_task = PythonOperator(
        task_id='download_inventory',
        python_callable=download_inventory
    )

    ingest_data_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )

    persist_data_task = PythonOperator(
        task_id='persist_data',
        python_callable=persist_data
    )

    generate_reports_task = PythonOperator(
        task_id='generate_reports',
        python_callable=generate_reports
    )

    download_orders_task >> download_inventory_task >> ingest_data_task >> transform_data_task >> validate_data_task >> persist_data_task >> generate_reports_task
    