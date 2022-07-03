from datetime import datetime, timedelta
import os
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.aws_hook import AwsHook

"""
The DAG provides a pipeline

"""

"""
Define Configuration
"""
S3_BUCKET = 'udacity-dend'
S3_PREFIX_DATA = 'song_data'
S3_PREFIX_LOG = 'log_data'
REDSHIFT_ID = 'redshift'
AWS_CONN = 'aws'


"""
 Init default arguments
"""

default_args = {
    'owner': 'danghoangkhai',
    'start_date': datetime(2022,4,7),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'email_on_retry': False
}
"""
 Init the DAG with options setting up
"""
dag = DAG('aws_redshift_dag',
          default_args=default_args,
          description='Extract data from S3 and transform to Redshift',
          schedule_interval='@hourly',
          catchup=False
          )



start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

schema_created = DummyOperator(task_id='Schema_created', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id= REDSHIFT_ID,
    aws_conn_id= AWS_CONN,
    s3_bucket= S3_BUCKET,
    s3_prefix=S3_PREFIX_DATA,
    table='staging_events',
    Cp_options="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id= REDSHIFT_ID,
    aws_conn_id= AWS_CONN,
    s3_bucket= S3_BUCKET,
    s3_prefix= S3_PREFIX_LOG,
    table='staging_songs',
    Cp_options="FORMAT AS JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    select_sql=SqlQueries.songplays_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table='users',
    select_sql=SqlQueries.users_table_insert,
    mode='truncate'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag, table='songs',
    select_sql=SqlQueries.songs_table_insert,
    mode='truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    table='artists',
    select_sql=SqlQueries.artists_table_insert,
    mode='truncate'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    select_sql=SqlQueries.time_table_insert,
    mode='truncate'
)
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    check_stmts=[
        {
            'sql': 'SELECT COUNT(*) FROM songplays;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM songplays WHERE songid IS NULL;',
            'op': 'eq',
            'val': 0
        }
    ]
)
create_staging_events_table = PostgresOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    postgres_conn_id= REDSHIFT_ID,
    sql=SqlQueries.staging_events_table_create
)
create_staging_songs_table = PostgresOperator(
    task_id='Create_staging_songs_table',
    dag=dag,
    postgres_conn_id= REDSHIFT_ID,
    sql=SqlQueries.staging_songs_table_create
)

create_songplays_table = PostgresOperator(
    task_id='Create_songplays_table',
    dag=dag,
    postgres_conn_id= REDSHIFT_ID,
    sql=SqlQueries.songplays_table_create
)

create_artists_table = PostgresOperator(
    task_id='Create_artists_table',
    dag=dag,
    postgres_conn_id= REDSHIFT_ID,
    sql=SqlQueries.artists_table_create
)

create_songs_table = PostgresOperator(
    task_id='Create_songs_table',
    dag=dag,
    postgres_conn_id= REDSHIFT_ID,
    sql=SqlQueries.songs_table_create
)

create_users_table = PostgresOperator(
    task_id='Create_users_table',
    dag=dag,
    postgres_conn_id= REDSHIFT_ID,
    sql=SqlQueries.users_table_create
)

create_time_table = PostgresOperator(
    task_id='Create_time_table',
    dag=dag,
    postgres_conn_id= REDSHIFT_ID,
    sql=SqlQueries.time_table_create
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)

# DAG dependencies
start_operator >> create_staging_songs_table
start_operator >> create_staging_events_table
start_operator >> create_songplays_table
start_operator >> create_artists_table
start_operator >> create_songs_table
start_operator >> create_users_table
start_operator >> create_time_table

create_staging_events_table >> schema_created
create_staging_songs_table >> schema_created
create_songplays_table >> schema_created
create_artists_table >> schema_created
create_songs_table >> schema_created
create_users_table >> schema_created
create_time_table >> schema_created

schema_created >> stage_events_to_redshift
schema_created >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator