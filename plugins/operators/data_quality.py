from symbol import comparison

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    An operator to check data are ingested properly. Raises an error if one table returns no result.
    """

    ui_color = '#89DA59'
    data_quality_checks = [
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null",
         'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null",
         'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null",
         'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null",
         'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null",
         'expected_result': 0}]

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for stmt in DataQualityOperator.data_quality_checks:
            result = redshift_hook.get_records(sql=stmt['check_sql'])[0][0]
            exp_result = stmt.get('expected_result')
            error_count = 0
            if result !=exp_result:
                error_count += 1
                self.log.info(f"This test {stmt['check_sql']} failed. Result is {result} and expected result is {exp_result})")
            elif error_count == 0:
                self.log.info(
                    f"This test {stmt['check_sql']} passed. Result is {result} and expected result is {exp_result})")


