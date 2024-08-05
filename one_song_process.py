# this module will take the artist_info as input, the platforms
# artist_info is a tuple: (artist_name, client_statement_file)
# then read the excel file based on the artist name. 
# then call the function artist_per_platform_process
import os, sys
import pickle
import logging
from pathlib import Path
import pandas as pd
from settings import  OUTPUT_PATH, PLATFORMS_DB_IN_SCOPE, get_conn_params, COLUMN_MAPPING, START_SONG_INDEX, END_SONG_INDEX, LOG_LEVEL, DataFeed, PLATFORM_IN_SCOPE_CLIENT_STATEMENT
# from settings import SUMMARY_OUTPUT_COLUMNS
from sql_template.queries import SQL_SONG  # the query template for select song name from platforms
from modules.rds_access import execute_sql_query_retriable, DatabaseQueryException

from modules.common import timeit
from collections import namedtuple
from modules.pc_platform import clean_song_data
from modules.refine_platform_song_rows import refine_platform_song_rows_v2

from modules.msv7 import preprocess_data, match_tracks_v2,  CLIENT_COLS, PLATFORM_COLS, save_result_to_excel_or_pickle

pd.options.mode.chained_assignment = None

SUMMARY_OUTPUT_COLUMNS = [ 'Total Comment', "Total Revenue", "cc_track", "versions", ]
DROP_COLUMNS = ['p_stream_count_1', 'p_stream_count_2', 'c_platform', 'c_revenue', 'match_id', 'p_track', 'c_track' ]

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
    if platform not in PLATFORMS_DB_IN_SCOPE: 
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
        return pd.DataFrame(columns=PLATFORM_COLS) 

    ######################################################################
    #### CLEAN
    # replace the column name to the standard column name based on the column mapping information
    df_platform_song = df_platform_song.rename(columns=COLUMN_MAPPING[platform])
    # print('---------------------')
    # print(df_platform_song.columns)
    # special process for qqmusicv2
    df_platform_song['p_artist'] = df_platform_song['p_artist'].apply(lambda x: ', '.join(x) if type(x) == list else x)

    # do the clean process for the platform data, add the columns pc_xxxxx
    df_platform_cleaned_song = clean_song_data(df_platform_song)
    # print('---------------------')
    # print(df_platform_song.columns)

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
    # print('---------------------')
    # print(df_platform_cleaned_song.columns)

    # reorg the columns in the cleaned_df_platform_song dataframe, the columns name should be sorted in asceding order
    df_platform_cleaned_song = df_platform_cleaned_song.loc[:, df_platform_cleaned_song.columns.sort_values()]

    return df_platform_cleaned_song 





@timeit
def process_one_song(song_index, song_name, df_client_song): 
    #######################################################################
    # firstly get the summary data from the input df_client_song
    df_summary_client = get_summary_data_from_client_df(df_client_song)
    logging.info("The summary data is based on the client statements is created for song {}".format(song_name))

    df_platform_concat_dict = {p: pd.DataFrame(columns=PLATFORM_COLS)  for p in PLATFORMS_DB_IN_SCOPE}

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
    data_feeds = [DataFeed(song_index, song_name, platform, artist_names, album_names, song_versions) for platform in PLATFORMS_DB_IN_SCOPE ]

    for data_feed in data_feeds: 
        ####################################################################################################################
        # Now do the refining process, 
        df_platform_cleaned_song= retrieve_clean_refine_platform_song_data(data_feed)
        df_platform_cleaned_song['p_platform'] = data_feed.platform
        df_platform_cleaned_song = df_platform_cleaned_song.loc[:, PLATFORM_COLS].reset_index(drop=True)
    
        logging.debug("The shape of df_platform_cleaned_song is {} after calling retrieve_clean_refine_platform_song_data".format(df_platform_cleaned_song.shape))
        # df_platform_cleaned_song.to_csv("output/debug/{}_y.csv".format(data_feed.platform))

        # try: 
        df_platform_concat_dict[data_feed.platform] = pd.concat([df_platform_concat_dict[data_feed.platform], df_platform_cleaned_song])
        # except Exception as e: 
        #     print(df_platform_cleaned_song.columns)
        #     print(df_platform_cleaned_song.T)

        #     print(df_platform_concat_dict[data_feed.platform].columns)

    
    # filter the columns for dataframe df_client_singer and df_platform, using CLIENT_COLS and PLATFORM_COLS
    df_client_song = df_client_song.loc[:, CLIENT_COLS]

    for platform in PLATFORMS_DB_IN_SCOPE: 
        if not df_platform_concat_dict[platform].empty:
            # df_platform_concat_dict[platform]['p_platform'] =  platform
            # df_platform_concat_dict[platform].reset_index(inplace=True, drop=True)
            # df_platform_concat_dict[platform].to_csv('output/debug/{}_x.csv'.format(platform))
            df_platform_concat_dict[platform] =  df_platform_concat_dict[platform].loc[:, PLATFORM_COLS]
            
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
        # sort the values based on the comments in descending order
        if not unmatched_df.empty:
            logging.info("now sort the values in unmatched dataframe based on the comments count...")
            unmatched_df = unmatched_df.sort_values(by=[ 'p_comments'], ascending=[ False])

        logging.info("The matched df shape is {}, unmatched df shape is {}".format(matched_df.shape, unmatched_df.shape))
        logging.info("now get the summary comments, likes, steam count from the platform data")

        # add the new column p_streams and then remove the unnecessary columns
        if not matched_df.empty: 
            matched_df['p_streams'] = matched_df.apply(
                    lambda row: 'NA' if (row['p_stream_count_1'] == 'NA' or row['p_stream_count_2'] == 'NA') 
                    else max([row['p_stream_count_1'], row['p_stream_count_2']]), 
                    axis =1)
            matched_df.drop(columns= DROP_COLUMNS, inplace=True)

            # move the p_streams column right after the p_likes_count column
            temp_cols = list(matched_df.columns)
            temp_cols.remove('p_streams')
            temp_cols.remove('refine_process_comment')
            temp_cols.remove('refine_similarity')

            temp_cols.insert(temp_cols.index('p_likes_count')+1, 'p_streams')
            temp_cols.append('refine_process_comment')
            temp_cols.append('refine_similarity')
            matched_df = matched_df.loc[:, temp_cols]

        # now create the summary df for the current song
        matched_summary_df = get_summary_from_platform_matched_df(matched_df)

        # now merge the match_summary_df with the summary data from the client statements
        logging.debug(str(df_summary_client))
        df_summary_client = df_summary_client.merge(matched_summary_df, on='cc_track', how='left')

        # add the total revenue and total comment columns
        df_summary_client['Total Revenue'] = df_summary_client.apply(
            lambda row: sum(row[idx] for idx in row.index if 'c_revenue' in idx), axis=1)
        df_summary_client['Total Comment'] = df_summary_client.apply(
            lambda row: sum(row[idx] for idx in row.index if 'p_comments' in idx), axis=1)

        # reorg the column sequence
        columns = SUMMARY_OUTPUT_COLUMNS + sorted([
            x for x in df_summary_client.columns if x not in SUMMARY_OUTPUT_COLUMNS]) 
        df_summary_client = df_summary_client.loc[:, columns]

        logging.info("now output the merged summary data to output folder")
        df_summary_client.to_excel(OUTPUT_PATH / "excel" / "summary_{}.xlsx".format('_'.join(song_name.split())), index=False)
        df_summary_client.to_pickle(OUTPUT_PATH / "pickle" / "summary_{}.pkl".format('_'.join(song_name.split())) )

        single_result_excel_path = OUTPUT_PATH / "excel" / "N{:06d}-{}-matched_v4.xlsx".format(song_index,'_'.join( song_name.split()))
        single_result_pickle_path = OUTPUT_PATH / "pickle" / "N{:06d}-{}.pkl".format(song_index,'_'.join( song_name.split()))

        save_result_to_excel_or_pickle(matched_df, unmatched_df, single_result_excel_path, output_pickle_path=single_result_pickle_path)

        # delete the snapshot file once the result is created 
        with open(OUTPUT_PATH / "restart_song_index.txt", 'w') as f:
            f.write(str(song_index+1))
        logging.info("============== The current song index '{}', song name '{}' is processed successfully. The restart_song_index file has been updated\n\n".format(song_index,song_name))
    except Exception as e:
        logging.error("Failed to process the artist '{}'".format(song_name))
        logging.info("Please solve the problem and then restart the applicatoin , python main.py restart")
        logging.exception(e)


def customized_summary(series):
    if any([x== 'NA' for x in series.values]):
        return 'NA'
        
    else: 
        return series.sum()


# get the summary comments, likes and streams based on the match_df
def get_summary_from_platform_matched_df(temp_df):
    if temp_df.empty: return pd.DataFrame()

    matched_df = temp_df.copy()
    matched_df = matched_df.loc[:, ['cc_track', 'p_platform', 'p_comments', 'p_likes_count', 'p_streams']]
    logging.debug(str(matched_df))
    # add dummary row for each in-scope platform
    song_name = list(matched_df['cc_track'])[0]
    for platform in PLATFORMS_DB_IN_SCOPE:
        matched_df = append_row(matched_df, {'cc_track': song_name, 'p_platform': platform, 'p_comments': 0, 'p_likes_count': 0, 'p_streams': 0})
    
    matched_summary = matched_df.groupby(['cc_track', 'p_platform'])[['p_comments', 'p_likes_count', 'p_streams']].agg(customized_summary)
    logging.debug(str(matched_summary))
    # matched_summary = matched_summary.rename(columns={'p_comments': 'comments', 'p_likes_count': 'likes'})
    # matched_summary.drop(columns=['streams_v1', 'streams_v2'], inplace=True)
    matched_summary = matched_summary.unstack()
    matched_summary.columns = [' '.join(reversed(col)).strip('_') for col in matched_summary.columns.values]
    matched_summary.reset_index(inplace=True)
    return matched_summary
    

# the following function is to get the summary information for a given song based on the client statements data
def append_row(df, row):
    return pd.concat([
                df, 
                pd.DataFrame([row], columns=row.keys())]
           ).reset_index(drop=True)

def get_summary_data_from_client_df(df_client_song_input):
    # TODO this can be changed when the requirements are changed
    # summary_by = ['Unique Song ID', 'Unique Version ID', 'c_platform']
    # summary_by = ['cc_track', 'cc_version', 'c_album']
    df_client_song = df_client_song_input.copy()
    summary_by = ['cc_track']
    values_tobe_summed = ['c_revenue']
    logging.debug("The list of versions are {}".format(df_client_song['cc_version'].unique()))

    versions = df_client_song['cc_version'].nunique()
    song_name = list(df_client_song['cc_track'])[0]
    logging.info("There are {} versions for song {}".format(versions, song_name))

    df_client_song['c_platform'] = df_client_song['c_platform'].apply(lambda x: x.strip().strip('"').lower())
    df_client_song = df_client_song.loc[df_client_song['c_platform'].isin(PLATFORM_IN_SCOPE_CLIENT_STATEMENT), :]
    df_client_song['cc_version'] = df_client_song['cc_version'].fillna('generic')


    # add a dummy row with zero value
    for platform in PLATFORM_IN_SCOPE_CLIENT_STATEMENT:
        dummy_row = {'cc_track': song_name, 'c_platform': platform, 'c_revenue': 0}
        df_client_song = append_row(df_client_song, dummy_row)

    df_client_song = df_client_song.loc[:, [*summary_by, 'c_platform', *values_tobe_summed]]
    #df_client_song.to_excel('temp/client_song_origin.xlsx')

    df_summary = df_client_song.groupby([*summary_by, 'c_platform'])[values_tobe_summed].agg('sum')
    df_summary = df_summary.pivot_table(index=summary_by, columns='c_platform', values=values_tobe_summed)
    df_summary.columns = [ ' '.join(reversed(cols)) for cols in df_summary.columns]

    df_summary.reset_index(inplace=True)
    df_summary['versions'] = versions

    # now rename the columns name 

    df_summary.to_excel('temp/client_song_summary.xlsx')
    df_client_song.to_excel('temp/client_song.xlsx')

    return df_summary

if __name__ == "__main__":

    # @timeit
    # def get_platform_data(platform, sql): 
    #     """
    #     Funtion to get the raw platform data from the platform database, based on the platform name and the song name
    #     input: platform name, song_name
    #     output: dataframe of the platform data for the song_name
    #     """
    #     if platform not in PLATFORMS: 
    #         logging.error("The platform '{}' is not in in-scope list, skipped...".format(platform))
    #         return None

    #     # get the sql statement template based on the platform name
    #     logging.info("The sql statement is: {}".format(sql))
    #     conn_param = get_conn_params(platform)

    #     # run the query to get the data based on the song name
    #     df_platform_song_raw = execute_sql_query_retriable(sql, conn_param)

    #     return df_platform_song_raw

    # # set the log level
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # # test the performance of the function get_platform_song_data
    # platform = 'netease_max'
    # song_name = 'mohicans'
    # sql1 = SQL_SONG[platform].format(song_name=song_name.replace("'", "''")) # when single quote included in the name, doubling the single quote
    # platform = 'netease_max_test'
    # sql2 = SQL_SONG[platform].format(song_name=song_name.replace("'", "''")) # when single quote included in the name, doubling the single quote
    # get_platform_song_data('netease_max', sql1)
    # get_platform_song_data('netease_max', sql2)
    
    from main import get_song_statement_data
  
    df_songs, songs = get_song_statement_data('cc_twosteps.pkl')
    platforms_in_client_statements = df_songs['c_platform'].unique()
    song_name = songs[0] 
    print(song_name)
    df_client_song = df_songs.loc[df_songs['cc_track'].str.strip("'").str.strip('"').str.strip().str.lower() == song_name.lower()]
    # print(df_client_song)
    
    get_summary_data_from_client_df(df_client_song)

    
