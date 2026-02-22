from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'edvard',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'database_maintenance_backup',
    default_args=default_args,
    description='Daily backup for DWH schemas',
    schedule_interval='0 3 * * *', # Launch every day at 03:00 am
    catchup=False,
    tags=['maintenance', 'backup'],
) as dag:

    run_backup = BashOperator(
        task_id='execute_backup_script',
        bash_command='/opt/airflow/scripts/backup.sh ', 
    )

    run_backup