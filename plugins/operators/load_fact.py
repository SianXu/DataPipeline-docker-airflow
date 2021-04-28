from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_sql = "",
                 insert_sql = "",
                 append = True, #this is for my own convenience in case i run multiple times
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        self.append = append

    def execute(self, context):
        '''
        this is to create the fact tables from staging tables
        although there's only 1 fact table here,
        in case there're more in the future, all sql are set as parameters
        '''

        self.log.info('Getting redshift info')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append is True:
            self.log.info("append mode on: append on existing records")
        else:
            self.log.info("append mode off: delete existing table")
            redshift.run(f'''
                DROP TABLE IF EXISTS {self.table}
            ''')

        self.log.info("create table")
        redshift.run(self.create_sql)

        self.log.info("insert table")
        complete_insert_sql = (f'''
            insert into {self.table}
            {self.insert_sql}
        ''')
        redshift.run(complete_insert_sql)
