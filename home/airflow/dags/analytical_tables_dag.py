from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator) 

from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries

start_date = datetime.utcnow()

default_args = {
    'owner': 'Jubin',
    'start_date': datetime(2019, 1, 5),
    'end_date': datetime(2019, 10, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'catchup' : False,
    'email_on_entry': False
}

dag = DAG('analytical_tables_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_redshift_tables = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id = "redshift"
)



stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table = "staging_events",
    aws_credential_id = "aws_credentials",
    redshift_conn_id = "redshift",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    file_format = "JSON",
    region="us-west-2",
    execution_date = start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table = "staging_songs",
    aws_credential_id = "aws_credentials",
    redshift_conn_id = "redshift",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    file_format = "JSON",
    region="us-west-2",
    execution_date = start_date
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    sql_query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    start_date= datetime(2019, 5, 1),
    table = "users",
    sql_query = SqlQueries.user_table_insert,
    truncate_table = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    start_date= datetime(2019, 5, 1),
    table = "songs",
    sql_query = SqlQueries.song_table_insert,
    truncate_table = True

)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id    ="redshift",
    _credentials_id = "aws_credentials",
    start_date= datetime(2019, 5, 1),
    table = "artists",
    sql_query = SqlQueries.artist_table_insert,
    truncate_table = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id ="redshift",
    aws_credentials_id = "aws_credentials",
    start_date= datetime(2019, 5, 1),
    table = "time",
    sql_query = SqlQueries.time_table_insert,
    truncate_table = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = {
            "FT":"songplays", 
            "DT1":"users", 
            "DT2":"songs", 
            "DT3":"artists", 
            "DT4":"time"
            }
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# 1.

start_operator >> create_redshift_tables 
create_redshift_tables >> stage_events_to_redshift
create_redshift_tables >> stage_songs_to_redshift

# 2.


stage_events_to_redshift >> load_songplays_table 
stage_songs_to_redshift >> load_songplays_table 

# 3.

load_songplays_table  >> load_user_dimension_table
load_songplays_table  >> load_song_dimension_table
load_songplays_table  >> load_artist_dimension_table
load_songplays_table  >> load_time_dimension_table

# 4.

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# 5.

run_quality_checks >> end_operator

