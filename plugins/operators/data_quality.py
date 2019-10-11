from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Start Data Quality checks')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        ## DIM_HOSTS TABLE
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM DIM_HOSTS WHERE HOST_ID IS NULL")
        if len(records) > 1 or len(records[0]) > 1:
            raise ValueError(f"Data quality check on table DIM_HOSTS failed due to an existing PRIMARY KEY(HOST_ID) with null values")
        num_records = records[0][0]
        if num_records >= 1:
            raise ValueError(f"Data quality check on table DIM_HOSTS failed due to an existing PRIMARY KEY(HOST_ID) with null values")
        logging.info(f"Data quality check on table DIM_HOSTS have completed successfully")
        ## DIM_REVIEWS TABLE
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM DIM_REVIEWS WHERE REVIEW_ID IS NULL")
        if len(records) > 1 or len(records[0]) > 1:
            raise ValueError(f"Data quality check on table DIM_REVIEWS failed due to an existing PRIMARY KEY(REVIEW_ID) with null values")
        num_records = records[0][0]
        if num_records >= 1:
            raise ValueError(f"Data quality check on table DIM_REVIEWS failed due to an existing PRIMARY KEY(REVIEW_ID) with null values")
        logging.info(f"Data quality check on table DIM_REVIEWS have completed successfully")
        ## FACT_AIRBNB_AMST TABLE
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM FACT_AIRBNB_AMST WHERE FACT_ID IS NULL")
        if len(records) > 1 or len(records[0]) > 1:
            raise ValueError(f"Data quality check on table FACT_AIRBNB_AMST failed due to an existing PRIMARY KEY(FACT_ID) with null values")
        num_records = records[0][0]
        if num_records >= 1:
            raise ValueError(f"Data quality check on table FACT_AIRBNB_AMST failed due to an existing PRIMARY KEY(FACT_ID) with null values")
        logging.info(f"Data quality check on table FACT_AIRBNB_AMST have completed successfully")