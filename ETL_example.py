from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from cuspide_scrape import extract_data
from cuspide_process import transform_data
from cuspide_load import load_data

default_args = {
    'owner':'Josias',
    'retries':'1',
    'retry_delay':timedelta(minutes=1)
    
}


with DAG(
    'ETL_example',
    default_args= default_args,
    description = 'extracting, transforming and loading data from Cuspide website',
    start_date = datetime(2023,2,24),
    schedule_interval='@daily' 
) as dag:
    task1 = PythonOperator(
        task_id='extract',
        python_callable = extract_data,
        dag=dag
    )
    task2 = PythonOperator(
          task_id='process',
          python_callable = transform_data,
          dag=dag
      )
    task3 = PythonOperator(
          task_id = 'load',
          python_callable=load_data,
          dag=dag
      )
    
    task1 >> task2 >> task3