from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

from src.extract import scrape_amazon_eg_data
from src.transform import transform_amazon_eg_data
from src.load import load_data
from src.load_snowflake import load_data_to_snowflake
from src.data_quality import check_warehouse_freshness, validate_clean_file

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'amazon_eg_etl',
    default_args=default_args,
    description='ETL DAG for Amazon Egypt Scraper',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['retail', 'amazon', 'scraper'],
) as dag:

    def extract_task(**kwargs):
        ti = kwargs['ti']
        file_path = scrape_amazon_eg_data()
        ti.xcom_push(key='raw_file_path', value=file_path)

    def transform_task(**kwargs):
        ti = kwargs['ti']
        raw_file_path = ti.xcom_pull(task_ids='scrape_amazon_eg_data', key='raw_file_path')
        clean_file_path = transform_amazon_eg_data(raw_file_path)
        ti.xcom_push(key='clean_file_path', value=clean_file_path)

    def load_task(**kwargs):
        ti = kwargs['ti']
        clean_file_path = ti.xcom_pull(task_ids='transform_amazon_eg_data', key='clean_file_path')
        load_data(clean_file_path)

    def load_snowflake_task(**kwargs):
        ti = kwargs['ti']
        clean_file_path = ti.xcom_pull(task_ids='transform_amazon_eg_data', key='clean_file_path')
        load_data_to_snowflake(clean_file_path)

    def data_quality_task(**kwargs):
        ti = kwargs['ti']
        clean_file_path = ti.xcom_pull(task_ids='transform_amazon_eg_data', key='clean_file_path')
        validate_clean_file(clean_file_path)

    def freshness_task():
        check_warehouse_freshness()

    t1 = PythonOperator(
        task_id='scrape_amazon_eg_data',
        python_callable=extract_task,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='transform_amazon_eg_data',
        python_callable=transform_task,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id='load_amazon_eg_data_to_postgres',
        python_callable=load_task,
        provide_context=True
    )

    dq = PythonOperator(
        task_id='validate_clean_product_data',
        python_callable=data_quality_task,
        provide_context=True
    )

    t4 = PythonOperator(
        task_id='load_amazon_eg_data_to_snowflake',
        python_callable=load_snowflake_task,
        provide_context=True
    )

    freshness = PythonOperator(
        task_id='check_warehouse_freshness',
        python_callable=freshness_task
    )

    t1 >> t2 >> dq >> [t3, t4] >> freshness
