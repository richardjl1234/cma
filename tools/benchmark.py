# This program is to do the benchmark for the query performance for different database/platforms
# the platforms/databases are: netease_max, kugou, qqmusicv2

# firstly import the necessary modules
import os
import time
from rds_access import execute_sql_query, DatabaseConnection
import pandas as pd

# create the decorate function to caltulate the time elapsed.
# def timeit(func):
#     def wrapper(*args, **kwargs):
#         start_time = time.time()
#         result = func(*args, **kwargs)
#         end_time = time.time()
#         print(f"Function {func.__name__} took {end_time - start_time} seconds to execute.")
#         return result
#     return wrapper


conn_params = {
    "host": "cma-new.c9wo4ke8qc15.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "user": os.environ.get("POSTGRE_USER", ''),
    "password": os.environ.get("POSTGRE_PWD", '') 
}

#################################
# kugou
cases = []
cases.append ({"database" :"kugou", 
        "sql_query" : """SELECT ks.*, ka.album_name
        FROM kugou_songs ks
        LEFT JOIN kugou_albums ka ON ks.album_id = ka.album_id
        WHERE 153413 = ANY(ks.singer_ids)
        OR 175750 = ANY(ks.singer_ids)
        OR 153666 = ANY(ks.singer_ids)        
        """
        })

# sql_query = "select count(*) from kugou_songs "

#################################
# qq database 
cases.append( {"database":"qqmusicv2", 
"sql_query" :"""
        SELECT song_mid, song_name, album_mid, album_name, artist_mid, singer_names,
        comment_number, singer_mids, company, release_date
        FROM songs
        WHERE '002Vj8o12l10eK' = ANY(singer_mids)
        or '001fbZH44WRuKW' = ANY(singer_mids)
        or '000Eqw823oivl5' = ANY(singer_mids)
        """
})
# sql_query = "select count(*) from songs "

################################
# netease
cases.append( { "database":  "netease_max", 
    "sql_query" : """
        SELECT album_id, song_id, song_name, artist_id, copyright_id, deprecated_artist_name, artist_ids,
        comment_count, audit_songs.artist_array, album_name, release_date, company, song_array
        FROM audit_songs
        INNER JOIN audit_albums USING (album_id)
        where '102714' = any(audit_songs.artist_array)
        or '792429' = any(audit_songs.artist_array)
        or '45336' = any(audit_songs.artist_array)
    """
})


results = []
def benchmark(dbname, sql_query):
    conn_params["database"] = dbname
    print(f"---------- {dbname} -----------------")
    with DatabaseConnection(conn_params) as conn:
        start = time.time()
        print("------ sql query result shape --------")
        print("sql_qyery: ", sql_query)
        df = execute_sql_query(sql_query, conn)
        print(df.shape)
        end = time.time()
        print("time cost: ", end - start)
        result = {"database": dbname, "time_cost": end - start, "shape": df.shape, "sql_query": sql_query}
        results.append(result)


# sql_query = "select count(*) from audit_songs "

for case in cases:
    benchmark(case["database"], case["sql_query"])

df = pd.DataFrame(results)
df.to_csv("benchmark_results.csv")


exit()

# now get the query result for qqmusicv2
conn_params["database"] = "qqmusicv2"   
print("---------- qqmusicv2 -----------------")
with DatabaseConnection(conn_params) as conn:

    start = time.time()
    print("------ sql query result shape --------")
    df = execute_sql_query(sql_query_qqmusicv2, conn)
    print(df.shape)
    end = time.time()
    print("time cost: ", end - start)

    start = time.time()
    print("------ sql query result first 10 rows --------")
    df = execute_sql_query(sql_query_qqmusicv2 + " limit 10", conn)
    # print(df)
    df.T.to_csv("qq.csv")
    end = time.time()
    print("time cost: ", end - start)

    start = time.time()
    print(" ----- the total count in the table ------")
    df = execute_sql_query("SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'songs' ", conn)
    print(df)
    end = time.time()
    print("time cost: ", end - start)

# now get the query result for kugou
conn_params["database"] = "kugou"
print("---------- kugou -----------------")
with DatabaseConnection(conn_params) as conn:

    start = time.time()
    print("------ sql query result shape --------")
    df = execute_sql_query(sql_query_kugou, conn)
    print(df.shape)
    end = time.time()
    print("time cost: ", end - start)

    start = time.time()
    print("------ sql query result first 10 rows --------")
    df = execute_sql_query(sql_query_kugou + " limit 10", conn)
    # print(df)
    df.T.to_csv("kugou.csv")
    end = time.time()
    print("time cost: ", end - start)

    start = time.time()
    print(" ----- the total count in the table ------")
    df = execute_sql_query("select count(*) from kugou_songs", conn)
    print(df)
    end = time.time()
    print("time cost: ", end - start)


# now get the query result for netease
conn_params["database"] = "netease_max"
print("---------- netease_max -----------------")
with DatabaseConnection(conn_params) as conn:

    start = time.time()
    print("------ sql query result shape --------")
    df = execute_sql_query(sql_query_netease_max, conn)
    print(df.shape)
    end = time.time()
    print("time cost: ", end - start)

    start = time.time()
    print("------ sql query result first 10 rows --------")
    df = execute_sql_query(sql_query_netease_max + " limit 10", conn)
    # print(df)
    df.T.to_csv("netease.csv")
    end = time.time()
    print("time cost: ", end - start)

    start = time.time()
    print(" ----- the total count in the table ------")
    df = execute_sql_query("select count(*) from audit_songs", conn)
    print(df)
    end = time.time()
    print("time cost: ", end - start)