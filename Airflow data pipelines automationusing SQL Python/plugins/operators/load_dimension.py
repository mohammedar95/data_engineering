from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id   = '',
                 table = '',
                 sql_load = '',
                 append_only=False,
                 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_load = sql_load
        self.append_only = append_only
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info("DELETE {} dimension table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))   
            
            
            
        self.log.info(f'LOAD DIMENSION TABLE  {self.table}')
        postgres.run(f'INSERT INTO {self.table} {self.sql_load}')
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_load
        )
        
        
        redshift.run(formatted_sql)
        self.log.info(F"Done: Copying {self.table} from {s3_path} to redshift")

    
        
        