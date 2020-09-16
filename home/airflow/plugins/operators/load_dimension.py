from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_stmt = """
        TRUNCATE TABLE {table}
    """

    @apply_defaults
    def __init__(self, redshift_conn_id= "",
                table= "" , sql_query ="", aws_credentials_id = "",
                truncate_table = False, 
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.truncate_table:
            self.log.info("Truncate the table before inserting the data !!")

            redshift.run(LoadDimensionOperator.truncate_stmt.format(
                table = self.table
            ))

        self.info.log("Data insertion into the Dimension table !!")
        redshift.run(self.sql_query.format(self.table))
        self.log.info(f"Success- {self.task_id}")
