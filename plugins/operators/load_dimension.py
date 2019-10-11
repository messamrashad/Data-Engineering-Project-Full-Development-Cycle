from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_template='{}'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 query="",
                 operation="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.query = query
        self.operation = operation
        self.table = table

    def execute(self, context):
        self.log.info('Starting load Dimension Tables')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        dimension_query = LoadDimensionOperator.sql_template.format(self.query)
        if self.operation == "insert":
            redshift_hook.run(dimension_query)
        else:
            redshift_hook.run(f"DELETE FROM {self.table}")