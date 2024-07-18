# this module will take the artist_info as input, the platforms
# artist_info is a tuple: (artist_name, client_statement_file)
# then read the excel file based on the artist name. 
# then call the function artist_per_platform_process
import os
import pickle
import logging
from pathlib import Path
import pandas as pd
# TODO, the SKIP_ARTIST, STOP_ARTIST need to be utilized 
from settings import LOG_PATH, OUTPUT_PATH, PLATFORMS, get_conn_params, COLUMN_MAPPING, START_DATA_FEED_IDX, START_ARTIST_INDEX, END_ARTIST_INDEX, END_DATA_FEED_IDX, ARTIST_NAMES, LOG_LEVEL
from sql_template.queries import SQL_SONG  # the query template for select song name from platforms
from modules.rds_access import execute_sql_query, DatabaseConnection, DatabaseQueryException

from modules.common import timeit
from collections import namedtuple
from modules.pc_platform import clean_song_data
from modules.msv7 import preprocess_data, match_tracks, save_to_excel, CLIENT_COLS, PLATFORM_COLS


log_name = LOG_PATH / 'artist_process.log'
# setup the logging to output the log to console and log file. 
logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    handlers=[logging.FileHandler(log_name, mode='a'),
                              logging.StreamHandler()
                              ]
                    )

@timeit
def get_artist_statement(artist_name, client_statement_file, input_folder= "input_data")   :
    ########################################################################## 
    # read the file_name into data frame
    logging.info("Read the excel file '{}' to memory...".format(client_statement_file))
    _, file_ext= os.path.splitext(client_statement_file)
    print(file_ext)

    input_file_full_path = Path(input_folder) / client_statement_file 

    if file_ext == '.pkl':
        with open(input_file_full_path, 'rb') as f:
            df_artists = pickle.load(f)
    elif file_ext == '.csv':
        df_artists = pd.read_csv(input_file_full_path)
    else: 
        df_artists = pd.read_excel(input_file_full_path)

    logging.info("The dataframe has {} rows".format(df_artists.shape[0]))

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
    with DatabaseConnection(conn_param) as conn:
        df_platform_song_raw = execute_sql_query(sql, conn )

        if df_platform_song_raw is None: 
            logging.error("ERROR WHEN QUERY THE DATA, PROGRAM ABORTED!")
            raise DatabaseQueryException

        if df_platform_song_raw.empty: 
            logging.warning("No rows returned for song name '{song_name}' from the platform '{platform}'".format(song_name=song_name, platform= platform))
            return pd.DataFrame()
        logging.info("{count} rows returned based on the song name '{song_name}' for the platform '{platform}'".format(
                count = df_platform_song_raw.shape[0], song_name=song_name, platform = platform))
    return df_platform_song_raw

def clean_refine_platform_song_data(data_feed):
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
    artist_name, song_name, platform 
    '''
    artist_name, song_name, platform = data_feed.artist_name, data_feed.song_name, data_feed.platform

    logging.info("Start to clean and refine the platform data for artist '{}' song '{}' on the platform '{}'".format(artist_name, song_name, platform))
    df_platform_song = get_platform_song_data(platform=platform, song_name=song_name)

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
    # TODO need to add the logic to refine the rows in the 
    # TODO need to add the logic to refine the rows in the 
    # TODO need to add the logic to refine the rows in the 
    # TODO need to add the logic to refine the rows in the 
    # TODO need to add the logic to refine the rows in the 
    df_platform_cleaned_song['refine_process_comment'] = ''
    df_platform_cleaned_song['refine_similarity'] = ''

    return df_platform_cleaned_song 

def main(artist_name, client_statement_file): 
    ############################################################################ 
    # get the singer statement information from the client statement excel sheet
    logging.info("----- Processing the artist '{}'".format(artist_name))
    df_client_singer = get_artist_statement(artist_name, client_statement_file)


    ########################################################################## 
    # get the disctict song names from the df_client_singer
    song_names = list(df_client_singer['cc_track'].fillna('').unique())

    ############################################################################ 
    # get the singer statement information from the client statement excel sheet
    ## TODO the album name list need to be created based on the song name
    ## TODO the album name list need to be created based on the song name
    ## TODO the album name list need to be created based on the song name
    # get the albums name based on the song name 
    # this is a dict, the key is the song_name, and the value is the album names list related to this song 
    album_names_dict  = {} 

    # TODO move the el dorado to the first element for testing
    # TODO move the el dorado to the first element for testing
    # TODO move the el dorado to the first element for testing
    if "el dorado" in song_names:
        song_names.remove("el dorado")
        song_names = ["el dorado"] + song_names


    logging.info("There are {} unique songs for artist '{}'".format(len(song_names), artist_name))
    logging.debug("The songs names are: {}".format('\n'.join(song_names)))

    # create the generator to generate the platform and song name
    DataFeed = namedtuple("DataFeed", ['artist_name', 'platform', 'song_name', 'album_names'])
    data_feeds = [DataFeed(artist_name, platform, song_name, album_names_dict.get(song_name, '')) 
                      for song_name in song_names if song_name.strip() !='' 
                      for platform in PLATFORMS ]

    # data_feed = next(data_feeds)
    df_platform_concated = pd.DataFrame()
    for seq_no, data_feed in enumerate(data_feeds): 
        logging.info("****** Current seq is {seq_no}, ".format(seq_no=seq_no))
        logging.debug("======The data feed is: {}".format(data_feed))
        if seq_no < START_DATA_FEED_IDX: 
            logging.warning("The start index is {start}, current seq number is {seq_no}, skip to next data feed....".format(start = START_DATA_FEED_IDX, seq_no=seq_no))
            continue

        if seq_no >= END_DATA_FEED_IDX: 
            logging.warning("The end index value is {end}, current seq number is {seq_no}, Now stop the iteration....".format(end = END_DATA_FEED_IDX, seq_no=seq_no))
            break


        df_platform_cleaned_song= clean_refine_platform_song_data(data_feed)
    
        ## Only when the LOG_LEVEL is DEBUG, output the file to output folder for check
        if LOG_LEVEL == logging.DEBUG:
            df_platform_cleaned_song.to_csv(OUTPUT_PATH/ "debug" / "{artist_name}-{platform}-{song_name}.csv".format(
                artist_name= data_feed.artist_name, 
                platform= data_feed.platform, 
                song_name = data_feed.song_name))

        df_platform_concated = pd.concat([df_platform_concated, df_platform_cleaned_song])

    
    # filter the columns for dataframe df_client_singer and df_platform, using CLIENT_COLS and PLATFORM_COLS
    df_client_singer = df_client_singer.loc[:, CLIENT_COLS]
    df_platform_concated =  df_platform_concated.loc[:, PLATFORM_COLS]


    df_client_singer, df_platform_concated = preprocess_data(df_client_singer, df_platform_concated) 

    ## when log_level is debug, then output the file to debug folder 
    if LOG_LEVEL == logging.DEBUG:
        df_platform_concated.to_csv(OUTPUT_PATH/ "debug"/ "PLATFORM_{}.csv".format('_'.join(artist_name.split())))
        df_client_singer.to_csv(OUTPUT_PATH/ "debug" / "CLIENT_{}.csv".format('_'.join(artist_name.split())))

    # matched_df, unmatched_df = match_tracks(client_df, platform_df)
    # save_to_excel(matched_df, unmatched_df, output_path)

if __name__ == "__main__":
    logging.info("<<<<< The program started >>>>>>")
    for artist_name in ARTIST_NAMES:
        main(artist_name, "cc_twosteps.pkl")
    