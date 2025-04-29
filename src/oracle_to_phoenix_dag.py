from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),  # Adjust as needed
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# instantiate the DAG
with DAG(
    dag_id='oracle_to_phoenix_etl_dag',
    default_args=default_args,
    description='ETL pipeline comparing Oracle tables to Phoenix and writing the results back to Phoenix',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # calls external Python script
    compare_and_transform = BashOperator(
        task_id='compare_and_transform',
        bash_command='python /path/to/your_script_folder/compare_and_transform.py '
                     '--oracle-host="YOUR_ORACLE_HOST" '
                     '--oracle-port="YOUR_ORACLE_PORT" '
                     '--oracle-service-name="YOUR_SERVICE_NAME" '
                     '--oracle-user="YOUR_ORACLE_USER" '
                     '--oracle-password="YOUR_ORACLE_PASSWORD" '
                     '--phoenix-url="YOUR_PHOENIX_JDBC_URL" '
                     '--phoenix-table="TARGET_PHOENIX_TABLE" ',
    )

    # if more tasks needed chain them
    compare_and_transform