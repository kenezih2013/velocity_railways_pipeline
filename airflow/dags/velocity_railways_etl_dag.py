from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
from utils import data_extraction, data_transformation, data_validation, data_loading

''' #Import Functions from python scripts
#from velocity_railways_etl import(
    #data_extraction,
    #data_transformation,
    #data_validation,
    #data_loading,
    #)'''

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': os.getenv('my_email'),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'velocity_railways_etl',
    default_args=default_args,
    description='velocity railways ETL pipeline with XCOM data transfer',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    extraction_task = PythonOperator(
        task_id='data_extraction',
        python_callable=data_extraction,
        provide_context=True,
        dag=dag,
    )

    transformation_task = PythonOperator(
        task_id='data_transformation',
        python_callable=data_transformation,
        provide_context=True,
        dag=dag,
    )

    validation_task = PythonOperator(
        task_id='data_validation',
        python_callable=data_validation,
        provide_context=True,
        dag=dag,
    )

    data_loading_task = PythonOperator(
        task_id='data_loading',
        python_callable=data_loading,
        provide_context=True,
        dag=dag,
    )

extraction_task >> transformation_task >> validation_task >> data_loading_task