from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {};
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id   = '',
                 table = '',
                 sql_load = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_load = sql_load
        
        
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_load
        )
        redshift.run(formatted_sql)
        self.log.info(F"Done: Copying {self.table} from {s3_path} to redshift")

    