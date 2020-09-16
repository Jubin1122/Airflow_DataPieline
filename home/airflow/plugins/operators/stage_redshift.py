from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
    copy {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    TIMEFORMAT AS 'epochmillisecs'
    {}'auto'
    """

    @apply_defaults
    def __init__(self, redshift_conn_id = "", aws_credentials_id = "",
    table = "", s3_bucket = "", s3_key = "", region= "", file_format = "JSON", *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region 
        self.file_format = file_format
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info(f"credentials.access_key: {credentials.access_key}")
        self.log.info(f"credentials.secret_key: {credentials.secret_key}")
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        
        s3_path = "s3://{}".format(self.s3_bucket)
        if self.execution_date:
            # Backfill a specific date
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            s3_path = '/'.join([s3_path, str(year), str(month), str(day)])
        s3_path = s3_path + '/' + self.s3_key
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format
        )
        redshift.run(formatted_sql)
                               
        self.log.info(f"Successfully stage: {self.table} from S3 to redshift")





