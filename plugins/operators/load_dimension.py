from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load the dimension table to Redshift. Loads with the truncate-insert pattern where
    the target table is emptied before the load.
    """

    ui_color = '#80BD9E'
    truncate_sql = """
        TRUNCATE TABLE {};
        """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_stmt="",
                 truncate_data=None,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.truncate_data=truncate_data

    def execute(self,context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_data:
            LoadDimensionOperator.truncate_sql.format(self.table)
            self.log.info(f"Truncated {self.table} from Redshift")
        self.log.info(f"Loading dimension table {self.table} to Redshift")
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_stmt})"
        self.log.info(self.sql_stmt)
        self.log.info(f"QUERY: {formatted_sql}")
        redshift.run(formatted_sql)
        self.log.info(f"Success: Loading dimension table {self.table} from to Redshift")
