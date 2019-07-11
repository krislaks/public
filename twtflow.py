from datetime import date, timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG(
        'twtflow',
        default_args = {
        'owner': 'airflow',
        'start_date': datetime(2019,7,10),
        'depends_on_past': False,
        'retries' : 1,
        'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(minutes=1),
) as dag:
        task_1 = BashOperator(
                task_id='task_1',
                bash_command='python3 /home/airflow/pyscripts/imptwtgres.py',

        )
        task_2 = BashOperator(
                task_id='task_2',
                bash_command='python3 /home/airflow/pyscripts/printstamp.py',
        )
        task_1 >> task_2
