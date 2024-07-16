# this module will take the artist_info as input, the platforms
# artist_info is a tuple: (artist_name, file_name)
# then read the excel file based on the artist name. 
# then call the function artist_per_platform_process
import logging
from pathlib import Path
import pandas as pd
from settings import LOG_PATH, OUTPUT_PATH, PLATFORMS, get_conn_params, COLUMN_MAPPING
from sql_template.queries import SQL_SONG  # the query template for select song name from platforms
from modules.rds_access import execute_sql_query, DatabaseConnection

from modules.common import timeit
from collections import namedtuple
from modules.pc_platform import clean_song_data

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
    # get the disctinct song_name from the df_singer.cc_track column, save to song_names, and make them to lower case
    df_singer = df_singer.fillna('')
    rows_empty_track_name = "{}.csv".format(artist_name + "_EMPTY_TRACK_NAME")
    logging.info("--------------- ROWS with EMPTY TRACK NAME are detected for artist '{}', those rows has been save to file '{}' ------".format(artist_name, rows_empty_track_name))
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
    sql = SQL_SONG[platform].format(song_name=song_name)
    logging.info("Now run the query from platform '{platform}' to get the rows for song name '{song_name}'".format(platform = platform, song_name = song_name))
    logging.debug("The sql statement is: {}".format(sql))
    conn_param = get_conn_params(platform)

    # run the query to get the data based on the song name
    with DatabaseConnection(conn_param) as conn:
        df_song = execute_sql_query(sql, conn )
        if df_song.empty: 
            logging.warning("No rows returned for song name '{song_name}' from the platform '{platform}'".format(song_name=song_name, platform= platform))
            return None
        logging.info("{count} rows returned based on the song name '{song_name}' for the platform '{platform}'".format(
                count = df_song.shape[0], song_name=song_name, platform = platform))
    return df_song


if __name__ == "__main__":
    
    ############################################################################ 
    # get the singer statement information from the client statement excel sheet
    artist_info = ("Two Steps From Hell", "cc_twosteps.xlsx") 
    df_singer = get_artist_statement(artist_info)

    ########################################################################## 
    # get the disctict song names from the df_singer
    song_names = df_singer['cc_track'].str.lower().unique()
    logging.info("There are {} unique songs for artist '{}'".format(len(song_names), artist_info[0]))
    logging.debug("The songs names are: {}".format('\n'.join(song_names)))

    # create the generator to generate the platform and song name
    DataFeed = namedtuple("DataFeed", ['platform', 'song_name'])
    data_feeds = iter([DataFeed(platform, song_name) 
                      for platform in PLATFORMS[:2]
                      for song_name in song_names[:1]])
    
    # data_feed = next(data_feeds)
    for data_feed in data_feeds: 
        df_platform_song = get_platform_data(platform=data_feed.platform, song_name=data_feed.song_name)

        # replace the column name to the standard column name based on the column mapping information
        df_platform_song = df_platform_song.rename(columns=COLUMN_MAPPING[data_feed.platform])

        # do the clean process for the platform data
        cleaned_df_platform_song = clean_song_data(df_platform_song)

        ## TODO the following line is temperory line, to be deleted
        cleaned_df_platform_song.to_csv(OUTPUT_PATH/"{platform}-{song_name}.csv".format(
            platform=data_feed.platform, song_name = data_feed.song_name))
    