from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id   = '',
                 dq_check_test = [],
                 tables = ""
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        
        self.redshift_conn_id   = redshift_conn_id
        self.dq_check_test      = dq_check_test
        self.tables             = tables
        
        

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        

        # test list of tests
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table};")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. and {self.table} has no results")
            records_num = records[0][0]
            if records_num < 1:
                raise ValueError(f"Data quality check failed. and {self.table} has 0 rows")
            self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")

        
        
        
