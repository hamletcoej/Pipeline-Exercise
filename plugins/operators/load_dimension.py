from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    sql_insert="""
            insert into {}
            {}
            """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dim_table="",
                 source_tbl_query="",
                 truncate=True,
                 #aws_credentials="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dim_table=dim_table
        self.source_tbl_query=source_tbl_query
        self.truncate=truncate        
        #self.aws_credentials=aws_credentials

    def execute(self, context):
        self.log.info('LoadDimensionOperator')
        #aws_hook=AwsHook(self.aws_credentials)
        #aws_credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
            
        self.log.info("Data insert from Staging tables to Dim Table")
        sql_stmt_dim=LoadDimensionOperator.sql_insert.format (self.dim_table,
                                                        self.source_tbl_query)
        
        redshift.run(sql_stmt_dim)

    