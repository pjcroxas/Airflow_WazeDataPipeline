from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

WAZE_METRO_MANILA = "https://www.waze.com/row-partnerhub-api/partners/13911541362/waze-feeds/208df417-b807-44f0-b255-60508e1a7fbf?format=1"

default_args = {
    'owner': 'pjroxas',
    'retries': 5, 
    'retry_delay': timedelta(minutes=2)
}

first_dag = DAG(
            dag_id="waze_metro_manila_dag",
            default_args=default_args,
            description="Extracting traffic data from waze feed",
            start_date=datetime(2021, 7, 29, 2),
            schedule_interval='@daily'
            )

initializer = BashOperator(
            task_id = 'initializer',
            bash_command = "echo Waze Data Engineering Pipeline",
            dag=first_dag
            )

def get_waze_feed():
     data = requests.get(WAZE_METRO_MANILA)
     return data.json()

extract_json = PythonOperator(
            task_id = 'extract_json',
            python_callable = get_waze_feed,
            dag=first_dag
            )
initializer >> extract_json