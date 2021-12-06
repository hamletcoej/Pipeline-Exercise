# /opt/airflow/start.sh
from datetime import datetime, timedelta
# import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import (LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

'''
Setting default args
'''
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1 ),
    'end_date': datetime(2018, 11, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False}

'''
Creating the DAG
'''
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *'
)

start_operator = DummyOperator(
    task_id='Begin_execution', 
    dag=dag
)

'''
Task for staging events log data to Redshift
'''
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Staging_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_path='s3://udacity-dend/log_data',
    region='us-west-2',
    json_option="s3://udacity-dend/log_json_path.json",
    provide_context=True,
    execution_date='start_date'
)


'''
Task for staging song data to Redshift
'''
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Staging_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    s3_path='s3://udacity-dend/song_data/A/A/A',
    region='us-west-2',
    provide_context=True,
    json_option='auto'
)

'''
Task for populating the songplay (fact) table
'''
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    fact_table='songplays',
    source_tbl_query=SqlQueries.songplay_table_insert,
    #aws_credentials="aws_credentials",
    dag=dag,
    truncate=False,
)

'''
Tasks for populating the users, song, artist, time tables (dimension)
'''
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    dim_table='users',
    source_tbl_query=SqlQueries.user_table_insert,
    aws_credentials="aws_credentials",
    dag=dag,
    truncate=False,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    dim_table='songs',
    source_tbl_query=SqlQueries.song_table_insert,
    #aws_credentials="aws_credentials",
    dag=dag,
    truncate=False,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    dim_table='artists',
    source_tbl_query=SqlQueries.artist_table_insert,
    #aws_credentials="aws_credentials",
    dag=dag,
    truncate=False,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    dim_table='time',
    source_tbl_query=SqlQueries.time_table_insert,
    #aws_credentials="aws_credentials",
    dag=dag,
    truncate=False,
)

'''
Data quality checks
'''
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    dag=dag
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)

'''
Task Dependencies
'''
start_operator>>[stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift]>>load_songplays_table
load_songplays_table>>[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >>run_quality_checks
run_quality_checks>>end_operator
