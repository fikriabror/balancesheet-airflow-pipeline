from airflow import DAG
from airflow.operators.python import PythonOperator
from etl_process import extract_data, transform_data, load_data, fact_balance_sheet
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os


default_args = {
    'owner': 'fikri',
    'retries': 1,
    'catchup': False,
    'start_date': datetime(2024, 9, 11),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='Extract data from excel, transform and load into PostgreSQL',
    schedule_interval='* 2 * * *',
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

fact_balance_task = PythonOperator(
    task_id='fact_balance_sheet',
    python_callable=fact_balance_sheet,
    provide_context=True,
    dag=dag
)

# task dependency
extract_task >> transform_task >> load_task >> fact_balance_task

