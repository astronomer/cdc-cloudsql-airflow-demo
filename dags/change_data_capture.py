#change_data_capture.py
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your-email@email.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
}

# Name DAG with the first argument
with DAG('change_data_capture_cloud_sql',
          max_active_runs=1, # Ensure one at a time
          schedule_interval=None, # Start with None for manual trigger, change to '@hourly'
          catchup=False, # If True, it will start historically to start_date
          start_date=datetime(2020, 12, 29, 0, 0, 0),
          default_args=default_args) as dag:
        
    # Fictitious tables you want to extract from 
    # You may load from a Variable or JSON/YAML files
    tables = ['table_1', 'table_2', 'table_3', 'table_4']
    
    kickoff_dag = DummyOperator(task_id='kickoff_dag')
        
    start_export = DummyOperator(
        task_id='start_export',
        trigger_rule='one_success'
    )
    
    complete_export = DummyOperator(
        task_id='complete_export',
        trigger_rule='all_success'
    )
    
    get_schema = BashOperator(
        task_id='get_schema',
        bash_command='echo "getting schema"',
        trigger_rule='one_success',
        pool = 'cloudsql_operations'
    )
     
    complete_dag = DummyOperator(task_id='complete_dag')

    for table in tables:

        export_table = BashOperator(
            task_id=f'export_{table}',
            bash_command=f'echo "exporting {table}"',
            trigger_rule='one_success',
            pool = 'cloudsql_operations'
        )

        get_schema >> export_table >> complete_export  
        
    kickoff_dag >> start_export >> get_schema
    complete_export >> complete_dag
