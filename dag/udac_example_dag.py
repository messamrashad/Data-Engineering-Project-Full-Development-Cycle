from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (CreateTablesOperator, StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

start_date = datetime.utcnow()

default_args = {
    'owner': 'udacity',
    'start_date': start_date,
    'catchup': False,
    'retry_delay': timedelta(minutes=3),
    'retries': '3',
    'depends_on_past': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1
         )
          #schedule_interval = "@hourly"
          #schedule_interval = '0 0 * * *')

start_operator = DummyOperator(
    task_id='START_OPERATOR',
    dag=dag)


##Creating STG Tables - DAGS

create_listings_staging_table = CreateTablesOperator (
    task_id='Create_Listings_STG_Table',
    dag=dag,
    query = SqlQueries.create_staging_listings,
    table = "STG_LISTINGS",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials"
)
create_listings_staging_table.set_upstream(start_operator)

create_calendars_staging_table = CreateTablesOperator (
    task_id='Create_Calendars_STG_Table',
    dag=dag,
    query = SqlQueries.create_staging_calendars,
    table = "STG_CALENDARS",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials"
)
create_calendars_staging_table.set_upstream(start_operator)

create_reviews_staging_table = CreateTablesOperator (
    task_id='Create_Reviews_STG_Table',
    dag=dag,
    query = SqlQueries.create_staging_reviews,
    table = "STG_REVIEWS",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials"
)
create_reviews_staging_table.set_upstream(start_operator)

create_neighbourhoods_staging_table = CreateTablesOperator (
    task_id='Create_Neighbourhoods_STG_Table',
    dag=dag,
    query = SqlQueries.create_staging_neighbourhoods,
    table = "STG_NEIGHBOURHOODS",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials"
)
create_neighbourhoods_staging_table.set_upstream(start_operator)

##Loading original events into STG Tables - DAGS

stage_listings_to_redshift = StageToRedshiftOperator(
    task_id='Stage_listings',
    dag=dag,
    table = "STG_LISTINGS",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "capstone-project-airbnb",
    s3_key = "listings_clean.csv",
    method = "csv"
)
stage_listings_to_redshift.set_upstream(create_listings_staging_table)

stage_calendars_to_redshift = StageToRedshiftOperator(
   task_id='Stage_calendars',
   dag=dag,
   table = "STG_CALENDARS",
   redshift_conn_id="redshift",
   aws_credentials_id = "aws_credentials",
   s3_bucket = "capstone-project-airbnb",
   s3_key = "calendars_clean.csv",
   method = "csv"
)
stage_calendars_to_redshift.set_upstream(create_calendars_staging_table)

stage_reviews_to_redshift = StageToRedshiftOperator (
  task_id='Stage_reviews',
  dag=dag,
  table = "STG_REVIEWS",
  redshift_conn_id="redshift",
  aws_credentials_id = "aws_credentials",
  s3_bucket = "capstone-project-airbnb",
  s3_key = "reviews_clean.csv",
  method = "csv"
)
stage_reviews_to_redshift.set_upstream(create_reviews_staging_table)

stage_neighbourhoods_to_redshift = StageToRedshiftOperator(
  task_id='Stage_neighbourhoods',
  dag=dag,
  table = "REF_NEIGHBOURHOODS",
  redshift_conn_id="redshift",
  aws_credentials_id = "aws_credentials",
  s3_bucket = "capstone-project-airbnb",
  s3_key = "Neighbourhoods.json",
  method = "FORMAT AS JSON 's3://capstone-project-airbnb/jpath.json'"
)
stage_neighbourhoods_to_redshift.set_upstream(create_neighbourhoods_staging_table)

##DUMMY OPERATOR JUST TO WAIT FOR EVERY STAGING TABLE HAS LOADED SUCCESSFULLY - DAG

MID_operator = DummyOperator(task_id='MID_OPERATOR',  dag=dag)

MID_operator.set_upstream(stage_listings_to_redshift)
MID_operator.set_upstream(stage_calendars_to_redshift)
MID_operator.set_upstream(stage_reviews_to_redshift)
MID_operator.set_upstream(stage_neighbourhoods_to_redshift)

##Creating FACT/DIMENSIONS Tables - DAGS

create_dim_hosts_table = CreateTablesOperator (
    task_id='Create_DIM_HOSTS_Table',
    dag=dag,
    query = SqlQueries.CREATE_DIM_HOSTS,
    table = "DIM_HOSTS",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials"
)
create_dim_hosts_table.set_upstream(MID_operator)

create_dim_reviews_table = CreateTablesOperator (
    task_id='Create_DIM_REVIEWS_Table',
    dag=dag,
    query = SqlQueries.CREATE_DIM_REVIEWS,
    table = "DIM_REVIEWS",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials"
)
create_dim_reviews_table.set_upstream(MID_operator)

create_dim_calendars_table = CreateTablesOperator (
    task_id='Create_DIM_CALENDARS_Table',
    dag=dag,
    query = SqlQueries.CREATE_DIM_CALENDARS,
    table = "DIM_CALENDARS",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials"
)
create_dim_calendars_table.set_upstream(MID_operator)

create_dim_properties_table = CreateTablesOperator (
    task_id='Create_DIM_PROPERTIES_Table',
    dag=dag,
    query = SqlQueries.CREATE_DIM_PROPERTIES,
    table = "DIM_PROPERTIES",
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials"
)
create_dim_properties_table.set_upstream(MID_operator)


##Loading modified events into STG Tables - DAGS
    
load_dim_hosts_table = LoadDimensionOperator(
  task_id='LOAD_DIM_HOSTS_TABLE',
  dag=dag,
  query = SqlQueries.hosts_table_insert,
  redshift_conn_id="redshift",
  aws_credentials_id = "aws_credentials",
  operation="insert",
  table="DIM_HOSTS"
)
load_dim_hosts_table.set_upstream(create_dim_hosts_table)

load_dim_reviews_table = LoadDimensionOperator(
  task_id='LOAD_DIM_REVIEWS_TABLE',
  dag=dag,
  query = SqlQueries.reviews_table_insert,
  redshift_conn_id="redshift",
  aws_credentials_id = "aws_credentials",
  operation="insert",
  table="DIM_REVIEWS"
)
load_dim_reviews_table.set_upstream(create_dim_reviews_table)

load_dim_properties_table = LoadDimensionOperator(
  task_id='LOAD_DIM_PROPERTIES_TABLE',
  dag=dag,
  query = SqlQueries.properties_table_insert,
  redshift_conn_id="redshift",
  aws_credentials_id = "aws_credentials",
  operation="insert",
  table="DIM_PROPERTIES"
)
load_dim_properties_table.set_upstream(create_dim_properties_table)

load_dim_calendars_table = LoadDimensionOperator(
  task_id='LOAD_DIM_CALENDARS_TABLE',
  dag=dag,
  query = SqlQueries.calendars_table_insert,
  redshift_conn_id="redshift",
  aws_credentials_id = "aws_credentials",
  operation="insert",
  table="DIM_CALENDARS"
)
load_dim_calendars_table.set_upstream(create_dim_calendars_table)

create_load_fact_airbnb_amst_table = LoadFactOperator(
  task_id='Create_Load_FACT_AIRBNB_AMST_Table',
  dag=dag,
  query = SqlQueries.CREATE_LOAD_FACT_AIRBNB_AMST,
  redshift_conn_id="redshift",
  aws_credentials_id = "aws_credentials"
)
create_load_fact_airbnb_amst_table.set_upstream(load_dim_hosts_table)
create_load_fact_airbnb_amst_table.set_upstream(load_dim_reviews_table)
create_load_fact_airbnb_amst_table.set_upstream(load_dim_properties_table)
create_load_fact_airbnb_amst_table.set_upstream(load_dim_calendars_table)

##RUN DATA QULAITY CHECKS TO ENSURE THAT RECORDS HAD BEEN MOVED CORRECTLY THROUGH PLATFORMS WITHOUT ANY ERRORS
run_quality_checks = DataQualityOperator(
    task_id='Run_DATA_QUALITY_Checks',
    dag=dag,
    redshift_conn_id="redshift"
)
run_quality_checks.set_upstream(create_load_fact_airbnb_amst_table)

##DUMMY OPERATOR to indicate that the DAG has run successfully - DAG

end_operator = DummyOperator(task_id='END_OPERATOR',  dag=dag)

end_operator.set_upstream(run_quality_checks)
