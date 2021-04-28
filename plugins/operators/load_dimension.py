from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_sql = "",
                 insert_sql = "",
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        self.truncate = truncate

    def execute(self, context):
        '''
        this is to create the dimension tables from staging tables
        '''

        self.log.info('Getting redshift info')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate is True:
            self.log.info("truncate mode on: dropt existing tables")
            redshift.run(f'''
                DROP TABLE IF EXISTS {self.table}
            ''')
        else:
            self.log.info("truncate mode off: append records")

        self.log.info("create table")
        redshift.run(self.create_sql)

        self.log.info("insert table")
        complete_insert_sql = (f'''
            insert into {self.table}
            {self.insert_sql}
        ''')
        redshift.run(complete_insert_sql)
