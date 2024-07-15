from modules.rds_access import execute_sql_query, DatabaseConnection, ConnParams
import os

conn_params = ConnParams(os.environ["POSTGRE_HOST"], 5432, "kugou",  os.environ["POSTGRE_USER"], os.environ["POSTGRE_PWD"] )
# conn_params = ConnParams(os.environ["POSTGRE_HOST"], 5432, "netease_max",  os.environ["POSTGRE_USER"], os.environ["POSTGRE_PWD"] )
# conn_params = ConnParams(os.environ["POSTGRE_HOST"], 5432, "qqmusicv2",  os.environ["POSTGRE_USER"], os.environ["POSTGRE_PWD"] )


sql_query = """SELECT singer_ids, singer_names from kugou_songs 
left join kugou_albums on kugou_songs.album_id = kugou_albums.album_id """

print('-----------------------------------------')
with DatabaseConnection(conn_params) as conn:
    df = execute_sql_query(sql_query, conn )
    print(df.shape)

df.to_csv('kugou_singer_id_names_mapping.csv')

