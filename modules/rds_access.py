# this module is to access the postgreSQL RDS database

# import modules
import psycopg2
import pandas as pd
from collections import namedtuple
import logging

keepalive_kwargs = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}

# define a customized class DatabaseQueryException
class DatabaseQueryException(Exception):
    pass


# define a namedtuple for db connection parameters
ConnParams = namedtuple('ConnParams', ['host', 'port', 'database', 'user', 'password'])

# function to execute sql query from postgresql database
# the result will be stored in dataframe
# input: 
#    1 sql statement
#    2. database connection
# output: 
#     dataframe
def execute_sql_query(sql, conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_names)
            return df
    except (Exception, psycopg2.Error) as error:
        logging.error("Error while executing SQL query:", error)
        return None

# context manager to connect to the database
# support the with statement
# input:
#    1. database connection parameters, it can be a dict or a nametuple
# output:
#     connection
class DatabaseConnection:
    def __init__(self, conn_params):
        # check the type of the parameters, if it is the number tuple, then change it to a dictionary, 
        self.conn_params = conn_params._asdict() if isinstance(conn_params, ConnParams) else conn_params

    def __enter__(self):
        self.conn = psycopg2.connect(**self.conn_params, **keepalive_kwargs)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    pass