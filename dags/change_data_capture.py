#change_data_capture.py
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os
from plugins.gcp_custom.operators.gcp_cloudsql_export_operator import (
    CloudSqlCsvExportOperator
)
from airflow.models import Variable

ROOT_DIR = os.getenv('AIRFLOW_HOME')
TEMPLATES_DIR = f'{ROOT_DIR}/include/templates'
dag_configs = Variable.get('cloud_sql_cdc_config',
                            deserialize_json=True)
gcp_conn_id = dag_configs.get('gcp_conn_id')
db_instance_id = dag_configs.get('db_instance_id')
gcp_project_id = dag_configs.get('gcp_project_id')
db_name = dag_configs.get('db_name')
gcs_bucket = dag_configs.get('gcs_bucket')
offload_export = dag_configs.get('offload_export')
gcs_root = f'{gcs_bucket}/cloudsql_export'

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
          schedule_interval = '@hourly', # Start with None for manual trigger, change to '@hourly'
          catchup=False, # If True, it will start historically to start_date
          template_searchpath=TEMPLATES_DIR, # Add this
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
    
    gcs_schema_keypath = (
        f"{gcs_root}/"
        "schemas/"
        "{{execution_date.year}}/"
        "{{execution_date.month}}/"
        "{{execution_date.day}}/"
        f"{db_name}/"
        f"{db_name}_schema_"
        "{{ts_nodash}}.csv"
    )

    get_schema = CloudSqlCsvExportOperator(
        task_id = 'get_schema',
        gcp_conn_id = gcp_conn_id,
        database_instance = db_instance_id,
        project_id = gcp_project_id,
        database_name = db_name,
        gcs_bucket = gcs_bucket,
        gcs_keypath = gcs_schema_keypath,
        offload = offload_export,
        params = {
            "tables": tables
        },
        export_sql = 'cloud_sql_cdc/export_csv/get_schema.sql',
        pool = 'cloudsql_operations',
        trigger_rule = 'one_success'
    )
     
    complete_dag = DummyOperator(task_id='complete_dag')

    for table in tables:

        gcs_raw_keypath = (
            f"{gcs_root}/"
            "raw/"
            f"{table}/"
            "{{execution_date.year}}/"
            "{{execution_date.month}}/"
            "{{execution_date.day}}/"
            f"{table}_"
            "{{ts_nodash}}.csv"
        )

        export_table = CloudSqlCsvExportOperator(
            task_id = f'export_{table}',
            gcp_conn_id = gcp_conn_id,
            database_instance = db_instance_id,
            project_id = gcp_project_id,
            database_name = db_name,
            gcs_bucket = gcs_bucket,
            gcs_keypath = gcs_raw_keypath,
            offload = offload_export,
            export_sql = f'cloud_sql_cdc/export_csv/{table}.sql',
            pool = 'cloudsql_operations',
            trigger_rule = 'one_success'
        )

        get_schema >> export_table >> complete_export
        
    kickoff_dag >> start_export >> get_schema
    complete_export >> complete_dag
