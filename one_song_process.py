# this module will take the artist_info as input, the platforms
# artist_info is a tuple: (artist_name, client_statement_file)
# then read the excel file based on the artist name. 
# then call the function artist_per_platform_process
import os, sys
import pickle
import logging
from pathlib import Path
import pandas as pd
from settings import  OUTPUT_PATH, PLATFORMS, get_conn_params, COLUMN_MAPPING, START_SONG_INDEX, END_SONG_INDEX, LOG_LEVEL, DataFeed
from sql_template.queries import SQL_SONG  # the query template for select song name from platforms
from modules.rds_access import execute_sql_query_retriable, DatabaseQueryException

from modules.common import timeit
from collections import namedtuple
from modules.pc_platform import clean_song_data
from modules.refine_platform_song_rows import refine_platform_song_rows_v2

from modules.msv7 import preprocess_data, match_tracks_v2, save_to_excel_v2, CLIENT_COLS, PLATFORM_COLS

pd.options.mode.chained_assignment = None


# @timeit
# def take_snapshot(artist_seq_no,data_feed_seq_no, df_platform_concat_dict):
#     snapshot_filename = OUTPUT_PATH / "snapshot.pkl"
#     # write the snapshot to pick file
#     with open(snapshot_filename, 'wb') as f: 
#         snapshot = (artist_seq_no, data_feed_seq_no, df_platform_concat_dict)
#         pickle.dump(snapshot, f)

#     logging.info("^^^^^^^ The snapshot data has been stored in the file {}, artist seq no is {}, data feed seq is {}".format(
#         str(snapshot_filename), artist_seq_no, data_feed_seq_no))

@timeit
def get_platform_song_data(platform, song_name): 
    """
    Funtion to get the raw platform data from the platform database, based on the platform name and the song name
    input: platform name, song_name
    output: dataframe of the platform data for the song_name
    """
    if platform not in PLATFORMS: 
        logging.error("The platform '{}' is not in in-scope list, skipped...".format(platform))
        return None

    # get the sql statement template based on the platform name
    sql = SQL_SONG[platform].format(song_name=song_name.replace("'", "''")) # when single quote included in the name, doubling the single quote
    logging.info("Now run the query from platform '{platform}' to get the rows for song name '{song_name}'".format(platform = platform, song_name = song_name))
    logging.debug("The sql statement is: {}".format(sql))
    conn_param = get_conn_params(platform)

    # run the query to get the data based on the song name
    df_platform_song_raw = execute_sql_query_retriable(sql, conn_param)

    if df_platform_song_raw is None: 
        logging.error("ERROR WHEN QUERY THE DATA, PROGRAM ABORTED!")
        raise DatabaseQueryException

    if df_platform_song_raw.empty: 
        logging.warning("No rows returned for song name '{song_name}' from the platform '{platform}'".format(song_name=song_name, platform= platform))
        return pd.DataFrame()
    logging.info("{count} rows returned based on the song name '{song_name}' for the platform '{platform}'".format(
            count = df_platform_song_raw.shape[0], song_name=song_name, platform = platform))

    return df_platform_song_raw

def retrieve_clean_refine_platform_song_data(data_feed):
    '''
    This function is to get the data from the platform database based on the song_name (ilike), 
    1. call the function get_platform_song_data to get the raw data from the platform database
    2. if the raw data is empty, return an empty dataframe
    3. otherwise, clean the data by calling the function clean_song_data
    4. reorg the columns in the cleaned_df_platform_song dataframe, the columns name should be sorted in asceding order
    5. Refine the rows based on the following criteria:
        a. remove the enclosing double quote and single quote in the pc_track column, remove the leading blanks in the pc_track column
        b. first round, get all the rows which pc_track is equal to the song_name. and artist name equal to artist name or album name equal to album name
        and get the additional artist name and album name from the pc_artist and pc_album columns. Add one more column the refine_process_comment, add the refine_similarity to EXACT MATCH
        c. second round, get all the rows which pc_track is equal to the song_name, artist name equal to alias atrist name or album name equal to alias name, add the information in the 
        refine_process_comment column as well., similarity to HIGHLY SIMILAR 
        d, third round,  

    6. return the refined dataframe
        a. keep the rows related to the artist name
        b. keep the rows related to the song name (ilike)
        c. keep the rows related to the platform name
        d. keep the rows related to the album name
    
    input: 
    artist_name, song_name, platform, album name list
    '''
    # unpack the data feed
    _, song_name, platform, artist_names,  album_names, song_version = data_feed

    logging.info("Start to retrieve, clean and refine the platform data for\n song name:'{}' artist names: '{}' on the platform: '{}', album names: '{}', version: '{}'".
                 format(song_name, ', '.join(artist_names), platform, ', '.join(album_names), song_version))

    ######################################################################
    #### RETRIEVE
    df_platform_song = get_platform_song_data(platform=platform, song_name=song_name)

    if df_platform_song.empty: 
        return df_platform_song 

    ######################################################################
    #### CLEAN
    # replace the column name to the standard column name based on the column mapping information
    df_platform_song = df_platform_song.rename(columns=COLUMN_MAPPING[platform])
    # special process for qqmusicv2
    df_platform_song['p_artist'] = df_platform_song['p_artist'].apply(lambda x: ', '.join(x) if type(x) == list else x)

    # do the clean process for the platform data, add the columns pc_xxxxx
    df_platform_cleaned_song = clean_song_data(df_platform_song)

    ## Only when the LOG_LEVEL is DEBUG, output the file to output folder for check
    if LOG_LEVEL == logging.DEBUG:
        df_platform_cleaned_song.to_pickle(OUTPUT_PATH/ "debug" / "{platform}-{song_name}.pkl".format(
            platform= data_feed.platform, 
            song_name = data_feed.song_name))
    
    ######################################################################
    # REFINEMENT 
    # now process the data and refine it for every specific platform 
    # only the rows with track name match to be used for the subsequence matching (v2)

    df_platform_cleaned_song= refine_platform_song_rows_v2(data_feed, df_platform_cleaned_song)

    # reorg the columns in the cleaned_df_platform_song dataframe, the columns name should be sorted in asceding order
    df_platform_cleaned_song = df_platform_cleaned_song.loc[:, df_platform_cleaned_song.columns.sort_values()]

    return df_platform_cleaned_song 

@timeit
def process_one_song(song_index, song_name, df_client_song): 

    df_platform_concat_dict = {p: pd.DataFrame()  for p in PLATFORMS}

    ############################################################################ 
    # get the albums name and artist names based on the song name 
    album_names = df_client_song['c_album'].str.lower().drop_duplicates().tolist() 
    album_names = tuple(map(lambda x: x.strip(), album_names))
    logging.info("The album names are:\n {}".format('\n '.join(album_names)))

    artist_names = df_client_song['cc_artist'].str.lower().drop_duplicates().tolist() 
    # split the artist name by comma, and flatten the list of list
    artist_names = map(lambda x: x.split(','), artist_names)
    artist_names = [item for sublist in artist_names for item in sublist]
    artist_names = tuple(map(lambda x: x.strip(), artist_names))

    logging.info("The artist names are:\n {}".format('\n '.join(artist_names)))

    # song versions 
    song_versions = df_client_song['cc_version'].fillna('generic').str.lower().drop_duplicates().tolist()
    song_versions = tuple(map(lambda x: x.strip(), song_versions))
    logging.info("The song versions are:\n {}".format('\n '.join(song_versions)))

    ############################################################################ 
    # create the generator to generate the platform and song name
    data_feeds = [DataFeed(song_index, song_name, platform, artist_names, album_names, song_versions) for platform in PLATFORMS ]

    for data_feed in data_feeds: 
        ####################################################################################################################
        # Now do the refining process, 
        df_platform_cleaned_song= retrieve_clean_refine_platform_song_data(data_feed)
        logging.debug("The shape of df_platform_cleaned_song is {} after calling retrieve_clean_refine_platform_song_data".format(df_platform_cleaned_song.shape))

        df_platform_concat_dict[data_feed.platform] = pd.concat([df_platform_concat_dict[data_feed.platform], df_platform_cleaned_song])

    
    # filter the columns for dataframe df_client_singer and df_platform, using CLIENT_COLS and PLATFORM_COLS
    df_client_song = df_client_song.loc[:, CLIENT_COLS]

    for platform in PLATFORMS: 
        if not df_platform_concat_dict[platform].empty:
            df_platform_concat_dict[platform]['p_platform'] =  platform
            # df_platform_concat_dict[platform] =  df_platform_concat_dict[platform].loc[:, PLATFORM_COLS]
            
    df_platform_concat_all = pd.concat(df_platform_concat_dict.values())

    try: 
        df_platform_concat_all = df_platform_concat_all.loc[:,PLATFORM_COLS ]
        df_client_song, df_platform_concat_all = preprocess_data(df_client_song, df_platform_concat_all ) 
    except Exception as e:
        logging.error("Failed to process '{}'".format(song_name))
        print(df_platform_concat_all.head())

    ## when log_level is debug, then output the file to debug folder 
    df_platform_concat_all.to_pickle(OUTPUT_PATH/ "debug"/ "PLATFORM_ALL_{}.pkl".format('_'.join(song_name.split())))
    df_client_song.to_pickle(OUTPUT_PATH/ "debug" / "CLIENT_{}.pkl".format('_'.join(song_name.split())))
    logging.info("The CLIENT and PLATFORM_ALL files have been outputted to the debug folder")


    try: 
        # To the final match between the client statements and the platform data
        matched_df, unmatched_df = match_tracks_v2(df_client_song,df_platform_concat_all)
        logging.info("The matched df shape is {}, unmatched df shape is {}".format(matched_df.shape, unmatched_df.shape))

        final_result_path = OUTPUT_PATH / "excel" / "N{:06d}-{}-matched_v4.xlsx".format(song_index,'_'.join( song_name.split()))
        final_pickle_path = OUTPUT_PATH / "pickle" / "N{:06d}-{}.pkl".format(song_index,'_'.join( song_name.split()))

        save_to_excel_v2(matched_df, unmatched_df, final_result_path, output_pickle_path=final_pickle_path)
        # delete the snapshot file once the result is created 
        with open(OUTPUT_PATH / "restart_song_index.txt", 'w') as f:
            f.write(str(song_index+1))
        logging.info("============== The current song index '{}', song name '{}' is processed successfully. The restart_song_index file has been updated\n\n".format(song_index,song_name))
    except Exception as e:
        logging.error("Failed to process the artist '{}'".format(song_name))
        logging.info("Please solve the problem and then restart the applicatoin , python main.py restart")
        logging.exception(e)
    



if __name__ == "__main__":

    @timeit
    def get_platform_data(platform, sql): 
        """
        Funtion to get the raw platform data from the platform database, based on the platform name and the song name
        input: platform name, song_name
        output: dataframe of the platform data for the song_name
        """
        if platform not in PLATFORMS: 
            logging.error("The platform '{}' is not in in-scope list, skipped...".format(platform))
            return None

        # get the sql statement template based on the platform name
        logging.info("The sql statement is: {}".format(sql))
        conn_param = get_conn_params(platform)

        # run the query to get the data based on the song name
        df_platform_song_raw = execute_sql_query_retriable(sql, conn_param)

        return df_platform_song_raw

    # set the log level
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # test the performance of the function get_platform_song_data
    platform = 'netease_max'
    song_name = 'mohicans'
    sql1 = SQL_SONG[platform].format(song_name=song_name.replace("'", "''")) # when single quote included in the name, doubling the single quote
    platform = 'netease_max_test'
    sql2 = SQL_SONG[platform].format(song_name=song_name.replace("'", "''")) # when single quote included in the name, doubling the single quote
    get_platform_song_data('netease_max', sql1)
    get_platform_song_data('netease_max', sql2)
    

