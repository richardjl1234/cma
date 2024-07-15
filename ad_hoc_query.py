#!/usr/bin/env python
# coding: utf-8

from rds_access import execute_sql_query, DatabaseConnection, ConnParams
import os
import pandas as pd
import functools

# function to filter the corresponding lines
def kugou_row_filter_template(row, singer_name=None, song_name=None):
    return row['ori_author_name'].lower().strip() == singer_name and song_name in row['work_name'].lower()

# function to get the filtered dataframe
# input is : conn_params, sql_query, singer_name, song_name
# output is: the filtered_df   
# enable the cache 
@functools.lru_cache(maxsize=128)
def get_filtered_df(conn_params, sql_query, singer_name, song_name):
    with DatabaseConnection(conn_params) as conn:
        df = execute_sql_query(sql_query, conn)
        print(f"--------- {singer_name=} {song_name= } -----------------------------")
        print("The shape of the output dataframe is: ", df.shape)
    
    row_filter = lambda row: kugou_row_filter_template(row, singer_name=singer_name, song_name=song_name)
    return df.loc[df.apply(row_filter, axis=1)]


if __name__ == "__main__":
    pd.set_option('display.max_rows', 5)                                            
    pd.set_option('display.max_columns', None) 

    # setup the connection parameters
    jconn_params = ConnParams(os.environ["POSTGRE_HOST"], 5432, "kugou",  os.environ["POSTGRE_USER"], os.environ["POSTGRE_PWD"] )
    # conn_params = ConnParams(os.environ["POSTGRE_HOST"], 5432, "netease_max",  os.environ["POSTGRE_USER"], os.environ["POSTGRE_PWD"] )
    # conn_params = ConnParams(os.environ["POSTGRE_HOST"], 5432, "qqmusicv2",  os.environ["POSTGRE_USER"], os.environ["POSTGRE_PWD"] )

    # setup the query sql 
    sql_query = """SELECT ks.*, ka.album_name
    FROM kugou_songs ks
    LEFT JOIN kugou_albums ka ON ks.album_id = ka.album_id
    WHERE work_name ilike '%lonely%'
    """

    filtered_df = get_filtered_df(conn_params, sql_query, 'nana', 'lonely')
    filtered_df.to_csv("temp_output/result_filtered.csv")
    print(filtered_df.head())

