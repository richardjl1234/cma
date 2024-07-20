# this module will take the artist_info as input, the platforms
# artist_info is a tuple: (artist_name, client_statement_file)
# then read the excel file based on the artist name. 
# then call the function artist_per_platform_process
import os, sys
import pickle
import logging
from pathlib import Path
import pandas as pd
# TODO, the SKIP_ARTIST, STOP_ARTIST need to be utilized 
from settings import LOG_PATH, OUTPUT_PATH, PLATFORMS, get_conn_params, COLUMN_MAPPING, START_DATA_FEED_IDX, START_ARTIST_INDEX, END_ARTIST_INDEX, END_DATA_FEED_IDX, ARTIST_NAMES, LOG_LEVEL
from sql_template.queries import SQL_SONG  # the query template for select song name from platforms
from modules.rds_access import execute_sql_query_retriable, DatabaseQueryException

from modules.common import timeit
from collections import namedtuple
from modules.pc_platform import clean_song_data, refine_logics, RefineLogicFuncNotDefined
from modules.msv7 import preprocess_data, match_tracks, save_to_excel, CLIENT_COLS, PLATFORM_COLS
import pickle


DataFeed = namedtuple("DataFeed", ['artist_seq_no', 'artist_name', 'platform', 'song_name', 'album_names'])
log_name = LOG_PATH / 'artist_process.log'
# setup the logging to output the log to console and log file. 
logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    handlers=[logging.FileHandler(log_name, mode='a'),
                              logging.StreamHandler()
                              ]
                    )

@timeit
def take_snapshot(artist_seq_no,data_feed_seq_no, df_platform_concat_dict):
    snapshot_filename = OUTPUT_PATH / "snapshot.pkl"
    # write the snapshot to pick file
    with open(snapshot_filename, 'wb') as f: 
        snapshot = (artist_seq_no, data_feed_seq_no, df_platform_concat_dict)
        pickle.dump(snapshot, f)

    logging.info("^^^^^^^ The snapshot data has been stored in the file {}, artist seq no is {}, data feed seq is {}".format(
        str(snapshot_filename), artist_seq_no, data_feed_seq_no))

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
    logging.debug("The column names in the df_singer are:\n {} \n".format(', '.join(df_singer.columns)))

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
    artist_name, song_name, platform, album name list
    '''
    # unpack the data feed
    artist_seq_no, artist_name, platform, song_name,  album_names = data_feed

    logging.info("Start to clean and refine the platform data for artist seq {} artist '{}' song '{}' on the platform '{}, album names are {}'".format(
        artist_seq_no, artist_name, song_name, platform, album_names))

    df_platform_song = get_platform_song_data(platform=platform, song_name=song_name)

    if df_platform_song.empty: 
        return df_platform_song 

    ######################################################################
    # REFINEMENT LOGIC
    # now process the data and refine it for every specific platform 
    logging.info("the shape of df_platform_song (before refinement) is {}: ".format(df_platform_song.shape))

    refine_logic_func =refine_logics(platform) 
    if refine_logic_func: 
        logging.info("The refine logic function is: {}".format(refine_logic_func.__name__))
        df_platform_song = refine_logic_func(data_feed, df_platform_song)
        logging.info("the shape of refined df_platform_song is {}".format(df_platform_song.shape))
    else: 
        raise RefineLogicFuncNotDefined

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

def process_one_artist(artist_seq_no, artist_name, client_statement_file, restart_snapshot = None): 
    if restart_snapshot is None: 
        df_platform_concat_dict = {p: pd.DataFrame()  for p in PLATFORMS}
    else: 
        restart_artist_seq_no, restart_data_feed_seq_no, df_platform_concat_dict = restart_snapshot


    ############################################################################ 
    # get the singer statement information from the client statement excel sheet
    df_client_singer = get_artist_statement(artist_name, client_statement_file)

    ########################################################################## 
    # get the disctict song names from the df_client_singer
    song_names = list(df_client_singer['cc_track'].fillna('').unique())

    ############################################################################ 
    # get the albums name based on the song name 
    # this is a dict, the key is the song_name, and the value is the album names list related to this song 
    album_names_dict  = { song_name:  
                         df_client_singer.loc[df_client_singer['cc_track'].str.lower() == song_name.lower()]['Album Name'].str.lower().drop_duplicates().tolist() 
                         for song_name in song_names}
    logging.debug("The album names dict is: {}".format(album_names_dict))

    # TODO move the el dorado to the first element for testing
    # TODO move the el dorado to the first element for testing
    # TODO move the el dorado to the first element for testing
    if "el dorado" in song_names:
        song_names.remove("el dorado")
        song_names = ["el dorado"] + song_names


    logging.info("There are {} unique songs for artist '{}'".format(len(song_names), artist_name))
    logging.debug("The songs names are: \n{}\n".format(', '.join(song_names)))

    # create the generator to generate the platform and song name
    data_feeds = [DataFeed(artist_seq_no, artist_name, platform, song_name, album_names_dict.get(song_name, '')) 
                      for song_name in song_names if song_name.strip() !='' 
                      for platform in PLATFORMS ]

    # data_feed = next(data_feeds)
    for data_feed_seq_no, data_feed in enumerate(data_feeds): 

        logging.info("****** Current artist seq is {artist_seq_no}, data feed seq is {data_feed_seq_no} \n".format(
            artist_seq_no = data_feed.artist_seq_no, data_feed_seq_no=data_feed_seq_no))
        logging.debug("======The data feed is: {}".format(data_feed))

        if data_feed_seq_no < (START_DATA_FEED_IDX if restart_snapshot is None else restart_data_feed_seq_no) and artist_seq_no <= (START_ARTIST_INDEX if restart_snapshot is None else restart_artist_seq_no): 
            logging.warning("The start data feed index is {start}, current data feed seq is {seq_no}, skipped ....".format(start = START_DATA_FEED_IDX, seq_no=data_feed_seq_no))
            continue

        if data_feed_seq_no > END_DATA_FEED_IDX and artist_seq_no >= END_ARTIST_INDEX: 
            logging.warning("The end index value is {end}, current seq number is {seq_no}, Now stop the iteration....".format(end = END_DATA_FEED_IDX, seq_no=data_feed_seq_no))
            break

        ####################################################################################################################
        # when the dict is created, take the snapshot to save the progress in case the long run program is killed
        take_snapshot(data_feed.artist_seq_no, data_feed_seq_no, df_platform_concat_dict)

        ####################################################################################################################
        # when some problem happens, the patially concated platform data for the artist will be storted in the pickle file
        try: 
            df_platform_cleaned_song= clean_refine_platform_song_data(data_feed)
        except DatabaseQueryException: 
            raise DatabaseQueryException  # early exit, and save the partial result file to pickle file
    
        ## Only when the LOG_LEVEL is DEBUG, output the file to output folder for check
        if LOG_LEVEL == logging.DEBUG:
            df_platform_cleaned_song.to_csv(OUTPUT_PATH/ "debug" / "{artist_name}-{platform}-{song_name}.csv".format(
                artist_name= data_feed.artist_name, 
                platform= data_feed.platform, 
                song_name = data_feed.song_name))

        df_platform_concat_dict[data_feed.platform] = pd.concat([df_platform_concat_dict[data_feed.platform], df_platform_cleaned_song])


    
    # filter the columns for dataframe df_client_singer and df_platform, using CLIENT_COLS and PLATFORM_COLS
    df_client_singer = df_client_singer.loc[:, CLIENT_COLS]

    for platform in PLATFORMS: 
        df_platform_concat_dict[platform] =  df_platform_concat_dict[platform].loc[:, PLATFORM_COLS]

    df_platform_concat_all = pd.concat(df_platform_concat_dict.values())


    df_client_singer, df_platform_concat_all = preprocess_data(df_client_singer, df_platform_concat_all ) 

    ## when log_level is debug, then output the file to debug folder 
    if LOG_LEVEL == logging.INFO:
        df_platform_concat_all.to_csv(OUTPUT_PATH/ "debug"/ "PLATFORM_ALL_{}.csv".format('_'.join( artist_name.split())))
        df_client_singer.to_csv(OUTPUT_PATH/ "debug" / "CLIENT_{}.csv".format('_'.join(artist_name.split())))
        logging.info("The CLIENT and PLATFORM_ALL files have been outputted to the debug folder")
        # delete the snapshot file once the result is created 
        


    # TODO un-comment the following 2 lines in the future
    # TODO, 
    # matched_df, unmatched_df = match_tracks(client_df, platform_df)
    # save_to_excel(matched_df, unmatched_df, output_path)
    # os.rename(OUTPUT_PATH / "snapshot.pkl", OUTPUT_PATH / "snapshot_{}.pkl".format('_'.join(artist_name.split())))   
    os.remove(OUTPUT_PATH / "snapshot.pkl" )   
    logging.info("============== The current artist '{}' is processed successfully. The snapshot file has been removed\n\n".format(artist_name))

def main():
    ## Resume the data from the pickle file which have already been processed, the pickle file name is snapshot.pkl
    if len(sys.argv) >= 2 and sys.argv[1] == 'restart':
        # read the pickle file snapshot.pkl, into the following variables
        logging.info("<<<<< The program RESTARTED >>>>>>")
        with open(OUTPUT_PATH / "snapshot.pkl", 'rb') as f: 
            restart_snapshot = pickle.load(f)
            restart_artist_seq_no, restart_data_feed_seq, _ = restart_snapshot
        
        logging.info("The snapshot file has been loaded, and the RESTART artist seq no is {}, RESTART Data feed seq is {}\n".format(
            restart_artist_seq_no, restart_data_feed_seq))
        # now the snapshot file has been loaded, rename it 
        os.rename(OUTPUT_PATH / "snapshot.pkl", OUTPUT_PATH / "snapshot_old.pkl")   
        logging.info("The snapshot file has been renamed to snapshot_old.pkl")

        for artist_seq_no, artist_name in enumerate(ARTIST_NAMES):
            logging.info("********* Processing the artist '{}', artist seq no is {} *********".format(artist_name, artist_seq_no))
            if artist_seq_no < restart_artist_seq_no or artist_seq_no > END_ARTIST_INDEX:
                logging.warning("The artist seq no {seq_no} is out of the range [{start}, {end}], skipped...".format(seq_no=artist_seq_no, start=START_ARTIST_INDEX, end=END_ARTIST_INDEX))
            else: 
                process_one_artist(artist_seq_no, artist_name, "cc_twosteps.pkl", restart_snapshot = restart_snapshot )

    else: 
        logging.info("<<<<< The program STARTED >>>>>>")
        for artist_seq_no, artist_name in enumerate(ARTIST_NAMES):
            logging.info("********* Processing the artist '{}', artist seq no is {} *********".format(artist_name, artist_seq_no))
            if artist_seq_no < START_ARTIST_INDEX or artist_seq_no > END_ARTIST_INDEX:
                logging.warning("The artist seq no {seq_no} is out of the range [{start}, {end}], skipped...".format(seq_no=artist_seq_no, start=START_ARTIST_INDEX, end=END_ARTIST_INDEX))
            else: 
                process_one_artist(artist_seq_no, artist_name, "cc_twosteps.pkl" )


if __name__ == "__main__":
    main()
    