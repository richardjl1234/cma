# this module will take the artist_info as input, the platforms
# artist_info is a tuple: (artist_name, file_name)
# then read the excel file based on the artist name. 
# then call the function artist_per_platform_process
import logging
from pathlib import Path
import pandas as pd
# TODO, the SKIP_ARTIST, STOP_ARTIST need to be utilized 
from settings import LOG_PATH, OUTPUT_PATH, PLATFORMS, get_conn_params, COLUMN_MAPPING, SKIP_DATA_FEED, SKIP_ARTIST, STOP_ARTIST, STOP_DATA_FEED, ARTIST_NAME
from sql_template.queries import SQL_SONG  # the query template for select song name from platforms
from modules.rds_access import execute_sql_query, DatabaseConnection, DatabaseQueryException

from modules.common import timeit
from collections import namedtuple
from modules.pc_platform import clean_song_data
from modules.msv7 import preprocess_data, match_tracks, save_to_excel, CLIENT_COLS, PLATFORM_COLS


log_name = LOG_PATH / 'artist_process.log'
# setup the logging to output the log to console and log file. 
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    handlers=[logging.FileHandler(log_name, mode='a'),
                              logging.StreamHandler()
                              ]
                    )

@timeit
def get_artist_statement(artist_info, input_folder= "input_data")   :
    ########################################################################## 
    # unpack the artist_info and store them in artist_name, file_name
    artist_name, file_name = artist_info
   
    ########################################################################## 
    # read the file_name into data frame
    logging.info("Read the execl file '{}' to memory...".format(file_name))
    excel_file_path = Path(input_folder) / file_name 
    df_artists = pd.read_excel(excel_file_path)

    ########################################################################## 
    # Get the data for the artist name only from the dataframe df_artists, the df_singer is for only one singer
    logging.info('Start to process the artist: {}'.format(artist_name))
    artists = df_artists['c_artist'].unique()
    logging.debug("There are {} rows in the dataframe, the list of the artist name are: {}".format(len(artists), ', '.join(artists)))
    df_singer = df_artists.loc[df_artists['c_artist'] == artist_name]
    logging.debug("The column names in the df_singer are: {} ".format('\n'.join(df_singer.columns)))

    ########################################################################## 
    # for those rows which has blank track name, output the report in the output folder
    # get the disctinct song_name from the df_singer.cc_track column, save to song_names, 
    df_singer = df_singer
    rows_empty_track_name = "{}.csv".format(artist_name + "_EMPTY_TRACK_NAME")
    logging.warning("^^^^ ROWS with EMPTY TRACK NAME are detected for artist '{}', those rows has been save to file '{}' ".format(artist_name, rows_empty_track_name))
    df_singer.loc[df_singer["cc_track"] == ''].to_csv(OUTPUT_PATH / rows_empty_track_name )

    return df_singer

@timeit
def get_platform_data(platform, song_name): 
    """
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
    with DatabaseConnection(conn_param) as conn:
        df_song = execute_sql_query(sql, conn )

        if df_song is None: 
            logging.error("ERROR WHEN QUERY THE DATA, PROGRAM ABORTED!")
            raise DatabaseQueryException

        if df_song.empty: 
            logging.warning("No rows returned for song name '{song_name}' from the platform '{platform}'".format(song_name=song_name, platform= platform))
            return pd.DataFrame()
        logging.info("{count} rows returned based on the song name '{song_name}' for the platform '{platform}'".format(
                count = df_song.shape[0], song_name=song_name, platform = platform))
    return df_song

def process_platform_song_df(artist_name, song_name, platform):
    '''
    This function is to get the data from the platform database based on the song_name (ilike), 
    then refine the results so that only the rows related to the artist name will be kept
    input: 
    '''
    df_platform_song = get_platform_data(platform=platform, song_name=song_name)

    if df_platform_song.empty: 
        return df_platform_song 

    # replace the column name to the standard column name based on the column mapping information
    df_platform_song = df_platform_song.rename(columns=COLUMN_MAPPING[platform])

    # do the clean process for the platform data
    df_platform_cleaned_song = clean_song_data(df_platform_song)
    
    # reorg the columns in the cleaned_df_platform_song dataframe, the columns name should be sorted in asceding order
    df_platform_cleaned_song = df_platform_cleaned_song.loc[:, df_platform_cleaned_song.columns.sort_values()]
    
    ##################################################################################
    # now process the platform rows so that only the rows related to the songs will be kept

    return df_platform_cleaned_song 


if __name__ == "__main__":
    logging.info("<<<<< The program started >>>>>>")
    
    ############################################################################ 
    # get the singer statement information from the client statement excel sheet
    artist_info = (ARTIST_NAME, "cc_twosteps.xlsx") 
    logging.info("----- Processing the artist '{}'".format(artist_info[0]))
    df_client_singer = get_artist_statement(artist_info)

    ########################################################################## 
    # get the disctict song names from the df_client_singer
    song_names = df_client_singer['cc_track'].fillna('').unique()
    logging.info("There are {} unique songs for artist '{}'".format(len(song_names), artist_info[0]))
    logging.debug("The songs names are: {}".format('\n'.join(song_names)))

    # create the generator to generate the platform and song name
    DataFeed = namedtuple("DataFeed", ['platform', 'song_name'])
    data_feeds = iter([DataFeed(platform, song_name) 
                      for platform in PLATFORMS
                      for song_name in song_names if song_name.strip() !=''])

    # data_feed = next(data_feeds)
    dfs_p = [] # dfs_p is a temperory list 
    for seq_no, data_feed in enumerate(data_feeds): 
        logging.info("** Current seq is {seq_no}, ".format(seq_no=seq_no))
        if seq_no < SKIP_DATA_FEED: 
            logging.warning("The skip value is {skip}, current seq number is {seq_no}, skip to next data feed....".format(skip = SKIP_DATA_FEED, seq_no=seq_no))
            continue

        if seq_no > STOP_DATA_FEED: 
            logging.warning("The stop value is {stop}, current seq number is {seq_no}, Now stop the iteration....".format(stop = STOP_DATA_FEED, seq_no=seq_no))
            break


        df_platform_cleaned_song= process_platform_song_df(artist_info[0], data_feed.song_name, data_feed.platform)
    
        ## TODO the following line is temperory line, to be deleted
        df_platform_cleaned_song.to_csv(OUTPUT_PATH/"{platform}-{song_name}.csv".format(
            platform=data_feed.platform, song_name = data_feed.song_name))

        dfs_p.append(df_platform_cleaned_song)

        

    # merge all the platform_song dataframe together 
    df_platform = pd.concat(dfs_p)
    
    # filter the columns for dataframe df_client_singer and df_platform, using CLIENT_COLS and PLATFORM_COLS
    df_client_singer = df_client_singer.loc[:, CLIENT_COLS]
    df_platform =  df_platform.loc[:, PLATFORM_COLS]


    df_client_singer, df_platform = preprocess_data(df_client_singer, df_platform) 

    ## TODO the following line is temperory line, to be deleted
    df_platform.to_csv(OUTPUT_PATH/"PLATFORM.csv")
    df_client_singer.to_csv(OUTPUT_PATH/"CLIENT_SINGER.csv")

    # matched_df, unmatched_df = match_tracks(client_df, platform_df)
    # save_to_excel(matched_df, unmatched_df, output_path)