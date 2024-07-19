# this module is to access the postgreSQL RDS database

# import modules
import psycopg2
import pandas as pd
from collections import namedtuple
import logging
import time

# create a database exception class, with erorr message xxxx

class DatabaseQueryException(Exception):
    pass

QUERY_RETRY_CNT = 3
SLEEP_TIME = 30

keepalive_kwargs = {
    "keepalives": 2,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}

# add the retry decorator so that the function will retry N times 
def retry(num_retries):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for _ in range(num_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Error executing {func.__name__}: {e}, sleep for {SLEEP_TIME} secords and retry...")
                    # Now sleep for 60 seconds
                    time.sleep(SLEEP_TIME)
                    continue
            logging.error(f"Maximum number of retries ({num_retries}) exceeded for {func.__name__}")
            raise DatabaseQueryException
        return wrapper
    return decorator


# define a customized class DatabaseQueryException


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
    with conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=column_names)
        return df


# when the query is retriable, we need to provide the conn_params instead of conn
@retry(num_retries=QUERY_RETRY_CNT)
def execute_sql_query_retriable(sql, conn_params):
    with DatabaseConnection(conn_params) as conn:
        return execute_sql_query(sql, conn)

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