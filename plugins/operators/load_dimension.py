from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
Class feature: Load data from AWS Redshift tables then store to Dimension table
Arguments : 
    - redshift_conn_id :  AWS Redshift connection ID
    - table : AWS Redshift target table name
    - select_sql: The SQL query for execute process.
    - mode : The mode of process
Output: Staging data in AWS Redshift is inserted from staging tables to Dimension table

"""
class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 select_sql='',
                 mode='append',
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        self.log.info(f'Start loading data from AWS Redshift tables into {self.table} to Dimension table...')
        if self.mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} dimension table...')
            redshift_hook.run(f'DELETE FROM {self.table};')
            self.log.info("Deletion complete.")

        sql = """
            INSERT INTO {table}
            {select_sql};
        """.format(table=self.table, select_sql=self.select_sql)
        redshift_hook.run(sql)
        self.log.info("Load data from AWS Redshift tables then store to Dimension table process is DONE ")