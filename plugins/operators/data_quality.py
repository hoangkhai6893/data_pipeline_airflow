from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
Class: The class to verify data quality of Redshift table.
Arguments:
    - redshift_conn_id : AWS Redshift connection id
Output : 
    PASS or FAIL 
"""
class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 check_tableReadshift=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_tableReadshift = check_tableReadshift
    
    """
    Function : Execute the process check quality
    """
    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        self.log.info("Execute the process data quality Operator")
        for _checktable in self.check_tableReadshift:
            result = int(redshift_hook.get_first(sql=_checktable['sql'])[0])

            # check if equal
            if _checktable['op'] == 'eq':
                if result != _checktable['val']:
                    raise AssertionError(f"Check equal failed : {result} {_checktable['op']} {_checktable['val']}")
            # check if not equal
            elif _checktable['op'] == 'ne':
                if result == _checktable['val']:
                    raise AssertionError(f"Check not equal failed: {result} {_checktable['op']} {_checktable['val']}")
            # check if greater than
            elif _checktable['op'] == 'gt':
                if result <= _checktable['val']:
                    raise AssertionError(f"Check greater failed: {result} {_checktable['op']} {_checktable['val']}")
            # check if less than
            elif _checktable['op'] == 'gt':
                if result >= _checktable['val']:
                    raise AssertionError(f"Check  less failed: {result} {_checktable['op']} {_checktable['val']}")
            self.log.info(f"Passed check: {result} {_checktable['op']} {_checktable['val']}")

        self.log.info("Check Redshift table quality is DONE")