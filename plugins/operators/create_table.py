from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator (BaseOperator):

    ui_color = '#F98866'
    sql_template='{}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info("Starting Creating %s Table",self.table)
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        sql_query = CreateTablesOperator.sql_template.format(self.query)
        redshift_hook.run(sql_query)
