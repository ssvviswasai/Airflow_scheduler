from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG execution times for 10pm and 11pm
execution_times = [
(12,32),
     (22,0)
]

for hour, minute in execution_times:
    dag_id = f'twice_daily_dag_{hour}_{minute}'
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        catchup=False,
        schedule_interval=f'{minute} {hour} * * *',  # Using cron-like format
    )
    
    transfer_data_sql = """
        INSERT INTO tpch_test.lineitem
        SELECT * FROM tpch.lineitem LIMIT 1000000
    """
    
    transfer_task = MySqlOperator(
        task_id=f'transfer_data_{hour}_{minute}',  # Include hour and minute in the task ID
        sql=transfer_data_sql,
        mysql_conn_id='tpch_connection',
        database='tpch',
        dag=dag,
    )
    
    # Add the DAG to the global namespace
    globals()[dag_id] = dag