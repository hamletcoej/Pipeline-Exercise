from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 aws_credentials="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials=aws_credentials

    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials)
        aws_credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        failure_list=[]
        
        self.log.info("Quality checks are applied to Tables")
        
        checks=[
        {'check_query': "SELECT COUNT(*) FROM staging_events WHERE song IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_events WHERE artist IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_events WHERE length IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_songs WHERE title IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_songs WHERE artist_name IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM staging_songs WHERE duration IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM songplays WHERE sessionid IS NULL", 'expected_result':0},
        {'check_query': "SELECT COUNT(*) FROM songplays WHERE start_time IS NULL", 'expected_result':0},
        ]
        
        for check in checks:
            query=check.get('check_query')
            expect_result=check.get('expected_result')
            result=redshift.get_records(query)[0]
            
            count_err=0
            
            if expect_result != result[0]:
                count_err = count_err+1
                failure_list.append(query)
            
        if count_err == 0:
            self.log.info('Data processed successfully')
        else:
            self.log.info ('Data processing failure')
            self.log.info (failure_list)
