from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 check_sql = 'SELECT COUNT(*) FROM ',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.check_sql = check_sql

    def execute(self, context):
        '''
        check table count, if no count raise error
        '''

        self.log.info('Getting redshift info')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        for table in self.tables:
            recs = redshift.get_records(f'''
                {self.check_sql} {table}
                '''
            )
            if (len(recs) < 1) or (len(recs[0]) < 0):
                self.log.error("table % is empty".format(table))
                raise ValueError(
                    "Table % failed data quality operator".format(table)
                )
            num_records = recs[0][0]
            if num_records == 0:
                self.log.error("No records found in {}".format(table))
                raise ValueError("No records found in {}".format(table))
            pass_msg = f"Data Quality check passed of table {table}, total count {num_records}"
            self.log.info(pass_msg)
