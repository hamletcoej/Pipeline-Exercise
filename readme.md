# SPARKIFY

## Motivation

Sparkify has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.


## Tech used

Built with:
- S3 storage
- Python
- Redshift


## Arcitechure

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

This data pipeline is dynamic and built from reusable tasks, that can be monitored, and allow easy backfills.
Data quality plays a big part when analyses are executed on top the data warehouse.  To ensure data quality this pipeline runs tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

Tables required:
stagingevents - log data from s3://udacity-dend/log_data
stagingsongs - song data from s3://udacity-dend/song_data

Fact Table
songplays - records in event data associated with song plays i.e. records with page NextSong

Dimension Tables
users - users in the app
songs - songs in music database
artists - artists in music database
time - timestamps of records in songplays broken down into specific units


# Running the Process

To run it the pipeline in airflow ensure connections are set up and turn the dag on - this will automatically start running the dag.

## Script Files

### dag
- udac_example_dag.py
This is the main file that imports the following files to execute the dag tasks and dependancies
### operations
- stage_redshift.py
Used for staging events log and song data to Redshift
- load_fact.py
Used for populating the songplay table in Redshift
- load_dimension.py
Used for populating the users, song, artist, time tables in Redshift
- data_quality.py
Used to run data quality checks
### helpers
- sql_queries.py
Queries used for inserting data into redshift tables
