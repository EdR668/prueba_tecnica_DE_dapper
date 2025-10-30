import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from validation.validator import validate_dataframe
from extraction.scraper import scrape_page
from persistence.writer import write_to_db
import pandas as pd

def task_extract(**kwargs):
    all_data = []
    for page in range(3):  # ejemplo: scrape 3 páginas
        all_data.extend(scrape_page(page))
    df = pd.DataFrame(all_data)
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())
    print(f"✅ Extracción completa: {len(df)} registros.")
    return True

def task_validate(**kwargs):
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(task_ids='extract', key='raw_data')
    df = pd.read_json(raw_json)
    valid_df, invalid_df = validate_dataframe(df)
    ti.xcom_push(key='validated_data', value=valid_df.to_json())
    print(f"✅ Validación completa: {len(valid_df)} válidos, {len(invalid_df)} descartados.")
    return True

def task_write(**kwargs):
    ti = kwargs['ti']
    validated_json = ti.xcom_pull(task_ids='validate', key='validated_data')
    valid_df = pd.read_json(validated_json)
    write_to_db(valid_df)
    print(f"✅ Escritura completada: {len(valid_df)} registros insertados.")
    return True

with DAG(
    'dapper_pipeline',
    start_date=datetime(2025, 10, 30),
    schedule_interval='@daily',
    catchup=False,
    tags=['dapper', 'etl']
) as dag:

    extract = PythonOperator(task_id='extract', python_callable=task_extract, provide_context=True)
    validate = PythonOperator(task_id='validate', python_callable=task_validate, provide_context=True)
    write = PythonOperator(task_id='write', python_callable=task_write, provide_context=True)

    extract >> validate >> write
