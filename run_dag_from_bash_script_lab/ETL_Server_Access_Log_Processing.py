# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Michael Musgrave',
    'start_date': days_ago(0),
    'email': ['michael@fake.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
# define the DAG
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='ETL as DAG for Processing Server Access Logs',
    schedule_interval=timedelta(days=1),
)

# define the task **extract_transform_and_load** to call shell script
#calling the shell script
process_server_access_logs = BashOperator(
    task_id="process_server_access_logs",
    bash_command="/home/project/airflow/dags/ETL_Server_Access_Log_Processing.sh ",
    dag=dag,
)

second_task = BashOperator(
    task_id="second_task",
    bash_command='echo "This is the second task."',
    dag=dag,
)

final_task = BashOperator(
    task_id="final_task",
    bash_command='echo "Lastly, this is the final task."',
    dag=dag,
)

# task pipeline
process_server_access_logs >> second_task >> final_task