from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 table              = '',
                 redshift_conn_id   = '',
                 aws_credentials_id = '',
                 s3_bucket          = '',
                 s3_key             = '',
                 file_type          = "",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table              = table
        self.redshift_conn_id   = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket          = s3_bucket
        self.s3_key             = s3_key
        self.file_type          = file_type

    def execute(self, context):
        # 1- setup the conn
        aws_hook    = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift    = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # 2- remove raw data
        self.log.info('DELET ALL DATA FROM DESTINATION')
        redshift.run("DELET FROM {}".format(self.table))
        
        # 3- add new data from s3 path
        rendered_key = self.s3_key.format(**context)
        s3_path      = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formated_sql   = StageToRedshiftOperator.copy_sql.format(
            table_name  = self.table,
            s3_path     = s3_path,
            access_key  = credentials.access_key,
            secret_key  = credentials.secret_key,
            file_type   = self.file_type
        )
        redshift.run(formated_sql)
        self.log.info(F"Done: Copying {self.table} from {s3_path} to redshift")

