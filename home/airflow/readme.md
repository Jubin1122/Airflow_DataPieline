# Sparkify Data Pipelines using Airflow

### Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Setup

- First we have to run the command `bash /opt/airflow/start.sh`

- This will start the Airflow Webserver and from there we can access the dag in the Airflow user interface.

#### AWS Connection
In the Airflow UI , we have to navigate to Admin/Connection and will create a connection entity called `aws_credentials`.

### Redshift connection
For this we have to follow the above mentioned navigation and create a connection entity `redshift`.

## Datasets
In this project, I have worked with two datasets. Here are the s3 links for each:

- Log data: `s3://udacity-dend/log_data`
- Song data: `s3://udacity-dend/song_data`

## Dag

Name of the Dag: `analytical_tables_dag.py`

![Alt text](/home/airflow/img/details_dag.png?raw=true "Optional Title")

These are some of the dag parameters:

1. There are no dependencies on the past.
2. On each failure task will be retried for 3 times,\
  5 minutes each.
3.  When the tasks are retried no email is sent.
4. It consists of a start date and end date.

```
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
```

Graphical Representation of the Pipeline Flow:

![Alt text](/home/airflow/img/Graph_view.png?raw=true "Optional Title")

**ETL STEPS:**
- First we will Begin with `DummyOperator` which is nothing but a starting point with no logical task.

- Then , I have used `PostgresOperator` to create tables.

- After yhat, I have built a custom operator called 'StageToRedshiftOperator' to stage data from S3(json format) to the Staging table called ***staging_events*** and ***staging_songs*** using AWS credentials and redshift connection.\
  Moreover, here we have set the context to true and execution date to start date.

- In the Load_songplays_fact_table step , I have created custom operator called `LoadFactOperator`. This will basically Load data from staging table *staging_events* to the analytical **fact table** called ***songplays***.

- Then using the custom operator `LoadDimensionOperator` I have performed the loading of data from staging tables to the analytical **dimension tables** namely; users, songs, artists, time.

- Finally, a *Run_data_quality_checks* task is performed using the custom operator, `DataQualityOperator`.
  - It will check two conditions; first whether there is any records or not.

  ```
  if len(records) < 1 or len(records[0]) < 1:
      raise ValueError(f"Data quality check failed. {table} returned no results")
  ```
  - Total number of records.

  ```
  num_records = records[0][0]
  if num_records < 1:
      raise ValueError(f"Data quality check failed. {table} contained 0 rows")
  self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
  ```
  - Moreover, here I am storing all the tables in a dictionary and using a for loop to iterate all the tables to calculate the number of records.

  ```
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
  ```
