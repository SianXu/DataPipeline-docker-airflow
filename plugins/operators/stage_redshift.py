from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="",
                 timeformat = "",
                 create_sql = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_format = json_format
        self.timeformat = timeformat
        self.create_sql = create_sql

    def execute(self, context):
        '''
        this is for the 2 staging tables, do the following
        - drop table if exists
        - create tables
        - copy data from s3

        '''

        self.log.info('Getting aws info')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("drop table if exists")
        redshift.run(f'''
            DROP TABLE IF EXISTS {self.table}
        ''')

        self.log.info("create table")
        redshift.run(self.create_sql)

        self.log.info("Copying data from S3 to Redshift")

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        formatted_sql =(f'''
            copy {self.table} from '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            {self.timeformat}
            region 'us-west-2' json {self.json_format}
        ''')
        redshift.run(formatted_sql)
