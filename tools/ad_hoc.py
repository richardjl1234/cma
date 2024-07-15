from rds_access import execute_sql_query, DatabaseConnection
import pandas as pd
import os
from pathlib import Path
import time

# get the POSTGRE related parameter from the os enviroments
# remember to run the command `source ../../cma_setup.sh` before run this program
conn_paras =     conn_params = {
        "host": os.environ.get("POSTGRE_HOST", ''),
        "port": 5432,
        # "database": "qqmusicv2",
        # "database": "kugou",
        # "database": "netease_max",
        "user": os.environ.get("POSTGRE_USER", ''),
        "password": os.environ.get("POSTGRE_PWD", '') 
    }

# get the database name from the prompt
print("Enter the database name[kugou, netease_max, qqmusicv2]:")
conn_params['database'] = input()

# the the sql query statement from the prompt, enter to accept the input
# get the multi line input from the promt, the keyword END will termiate the input
print("Enter the SQL query (press enter on a new line, 'END' to terminate):")
lines = []
while True:
    line = input()
    if line.strip().upper() == 'END':
        break
    lines.append(line)
sql_query = '\n'.join(lines)

# cacaluate the time spent in the following query   
start = time.time()
with DatabaseConnection(conn_params) as conn:
    print("--------------------------------------")
    print("SQL query:")
    print(sql_query)
    df = execute_sql_query(sql_query, conn)
    print(df.shape)

end = time.time()   
print(f"Time spent: {end - start} seconds")

# setup the pandas dataframe print options, limit rows to be 5, and no limitatoin on the column width
pd.set_option('display.max_rows', 5)
pd.set_option('display.max_columns', None)

# print the dataframe to the console,  the first 5 rows
if df.shape[0] < 100: 
    print("the count of rows less then 100, all the rows are displayed..")
else: 
    print("THE COUNT OF ROWS more THAN 100, ONLY THE FIRST 100 ROWS ARE DISPLAYED!...")

print(df.head(100))

# get the file name from the prompt, if the file name is not provided, use the default name "result.csv"
print("Enter the file name to save the result to folder temp_output (press enter to use default filename 'result.csv'):")
path = Path("temp_output")
fname = input()
file_name = path / (fname if fname else "result.csv")

# get the input from prompt to determin if output file should be transposed.
print("Do you want to transpose the result? (y/n):")
transpose = input().lower() == 'y'

if transpose: 
    df.T.to_csv(file_name)
else: 
    df.to_csv(file_name, index=False)
