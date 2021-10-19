from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Mohammed',
    'start_date': datetime(2019, 1, 12),
    'retries' : 3,                               # when failed, the task will try 3 times.
    'retry_delay': timedelta(minutes=5),         #  time interval between each retry
    'depends_on_past': False,                    # DAG will not be depend on past tasks

}

dag = DAG('mar_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup = False,           
        )



start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = "staging_events",
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_default',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json',
    file_type = "JSON 's3://udacity-dend/log_json_path.json'"
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_default',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data/{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json',
    file_type = "JSON 's3://udacity-dend/song_json_path.json'"
    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_load = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_load = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_load = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag, 
    redshift_conn_id="redshift",
    table="artists",
    sql_load = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_load = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag, 
    redshift_conn_id="redshift",
    tables=[ "songplays", "songs", "artists",  "time", "users"]
    
)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> [stage_events_to_redshift,
                   stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table,
                                                                         load_song_dimension_table,
                                                                         load_artist_dimension_table,
                                                                         load_time_dimension_table] >> run_quality_checks >> end_operator









