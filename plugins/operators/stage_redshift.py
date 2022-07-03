
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook

"""
Class feature : Copy data source from AWS S3 then store to AWS Redshift
Arguments: 
    - redshift_conn_id : AWS Redshift connection ID
    - aws_conn_id : AWS S3 credentials
    - table : AWS Redshift target table name
    - s3_bucket : AWS S3 bucket name
    - s3_prefix : AWS S3 prefix name
    - Cp_options : the copy options 
    - ignore_headers : ignote headers in CSV data
    - use_partitioned_data : is source data in partitioned structure
Ouput : Source data is COPIED to AWS Redshift staging tables.
"""

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id="redshift",
                 aws_conn_id="aws",
                 table="",
                 s3_bucket="",
                 s3_prefix="",
                 Cp_options='',
                 ignore_headers=1,
                 use_partitioned_data="False",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.Cp_options = Cp_options

    """
    Setup  AWS S3 and Redshift connections
    """
    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info(f'Preparing process copy data  from {self.s3_bucket}/{self.s3_prefix} to {self.table} table...')
        copy_query = """
                    COPY {table}
                    FROM 's3://{s3_bucket}/{s3_prefix}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {Cp_options};
                """.format(table=self.table,
                           s3_bucket=self.s3_bucket,
                           s3_prefix=self.s3_prefix,
                           access_key=credentials.access_key,
                           secret_key=credentials.secret_key,
                           Cp_options=self.Cp_options)
        self.log.info('Start process Copy data...')
        redshift_hook.run(copy_query)
        self.log.info("Process Copy data source from AWS S3 then store to AWS Redshift is Done .")

