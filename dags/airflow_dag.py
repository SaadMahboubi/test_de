import sys
import os

# Ajouter le répertoire src au PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pipeline

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_drugs',
    default_args=default_args,
    description='Pipeline de traitement des données de drugs et publications',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def run_pipeline():
    pipeline.main()

run_pipeline_task = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_pipeline,
    dag=dag,
)

run_pipeline_task
