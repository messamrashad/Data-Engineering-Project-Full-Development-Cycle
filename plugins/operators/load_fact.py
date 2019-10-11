from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_template='{}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info('Starting load Fact Table')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        fact_query = LoadFactOperator.sql_template.format(self.query)
        redshift_hook.run(fact_query)