from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
Class feature : Load data from AWS Redshift tables then store to Fact table
Arguments:
 - redshift_conn_id : AWS Redshift connection ID
 - table : AWS Redshift target table name
 - select_sql : The SQL query for execute process.
Output: Staging data in AWS Redshift is inserted from staging tables to Fact table.
"""

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 select_sql='',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql
    """
    Function : Execute process loading data 
    """
    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        self.log.info(f'Start loading data from AWS Redshift tables into {self.table} to  fact table...')
        sql = """
            INSERT INTO {table}
            {select_sql};
        """.format(table=self.table, select_sql=self.select_sql)

        redshift_hook.run(sql)

        self.log.info("Load data from AWS Redshift tables then store to Fact table process is DONE ")