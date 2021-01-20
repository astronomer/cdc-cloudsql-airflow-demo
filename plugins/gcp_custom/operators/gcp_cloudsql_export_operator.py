import re
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.gcp_sql_hook import CloudSqlHook

class CloudSqlCsvExportOperator(BaseOperator):
    """
    Call CloudSQL Export API and direct output to a GCS Bucket. Export 
    can be scoped by a SELECT export_query. Subqueries are acceptable but
    CTE-type queries will be rejected. Optionally, for a big load, one can 
    offload to a serverless format. Additional pricing and time will be
    be incurred for the tradeoff of complete removal of database load.

    The API call is asynchronous; however, the hook has a 
    _wait_for_operation_to_complete function built in. We can assume if it 
    completes without error, the task has succeeded.

    For more information see the Export API 
    https://cloud.google.com/sql/docs/mysql/import-export/exporting
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param gcs_bucket: The name of the GCS bucket.
    :type gcs_bucket: str
    :param gcs_keypath: The keypath in the GCS Bucket to the destination object.
    :type gcs_keypath: str
    :param project_id: The GCP project ID
    :type project_id: str
    :param database_instance: The CloudSQL Instance ID
    :type database_instance: str
    :param database_name: The name in CloudSQL of the database
    :type database_name: str
    :param export_sql: SQL SELECT statement for export selection
        :type export_sql: str
    :param offload: True for serverless export, False otherwise
        :type offload: bool
    """

    template_fields = ('export_sql','gcs_bucket','gcs_key','project_id')

    template_ext = ('.sql', )

    ui_color = '#9f8fdb'

    @apply_defaults
    def __init__(self, gcs_bucket, gcs_keypath, database_instance,
                database_name, export_sql, project_id,
                gcp_conn_id='gcs_default', offload=False, 
                *args, **kwargs):
        
        super().__init__(*args, **kwargs)
        
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_keypath = gcs_keypath
        self.database_instance = database_instance
        self.database_name = database_name
        self.export_sql = export_sql
        self.project_id = project_id
        self.offload = offload
        self.file_type = 'CSV'

    def execute(self, context):
        try:
            assert self.file_type == 'CSV'
        except AssertionError:
            raise AirflowException(f'Extract file_type must be CSV')

        hook = CloudSqlHook(api_version='v1beta4', gcp_conn_id=self.gcp_conn_id)

        newlines = re.compile("[\r\n]+(\s+|)")
        
                # The query must have newlines removed
        self.export_sql = re.sub(newlines, ' ', self.export_sql)\
                            .strip()

        body = {
            "exportContext":
            {
              "fileType": f"{self.file_type}",
              "uri": f"gs://{self.gcs_bucket}/{self.gcs_keypath}",
              "databases": [f"{self.database_name}"], 
              "offload": self.offload, # True for serverless (takes longer but offloads - use true for historical extract)
              "csvExportOptions":
               {
                   "selectQuery": f"{self.export_sql}"
               }
            }
        }
        hook.export_instance(instance=self.database_instance,
                             body=body, 
                             project_id=self.project_id)
