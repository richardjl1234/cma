import os, sys
import pickle
import logging
from settings import LOG_PATH, OUTPUT_PATH, START_SONG_INDEX, END_SONG_INDEX, LOG_LEVEL, INPUT_FILE, INPUT_PATH
from one_song_process import process_one_song
from modules.common import timeit, FUNC_TIME_DICT
from pathlib import Path
import pandas as pd

# get the current date in format yyyymmdd
from datetime import datetime
today = datetime.today().strftime('%Y%m%d')

log_name = LOG_PATH / 'song_audit_{}.log'.format(today)
# setup the logging to output the log to console and log file. 
logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    handlers=[logging.FileHandler(log_name, mode='a'),
                              logging.StreamHandler()
                              ]
                    )

@timeit
def get_song_statement_data(client_statement_file, input_folder= INPUT_PATH)   :
    ########################################################################## 
    # read the file_name into data frame
    logging.info("Read the input file '{}' into memory...".format(client_statement_file))
    _, file_ext= os.path.splitext(client_statement_file)
    # print(file_ext)

    input_file_full_path = Path(input_folder) / client_statement_file 

    if file_ext == '.pkl':
        with open(input_file_full_path, 'rb') as f:
            df_songs = pickle.load(f)
    elif file_ext == '.csv':
        df_songs = pd.read_csv(input_file_full_path)
    else: 
        df_songs = pd.read_excel(input_file_full_path)

    logging.info("The dataframe of the input client statements has {} rows".format(df_songs.shape[0]))

    ########################################################################## 
    # for those rows which has blank track name, output the report in the output folder
    # get the disctinct song_name from the df_singer.cc_track column, save to song_names, 
    rows_empty_track_name = "{}.csv".format("EMPTY_TRACK_NAME")
    df_songs['cc_track'] = df_songs['cc_track'].fillna('')
    df_empty_songs = df_songs.loc[df_songs["cc_track"].str.strip() == '']
    df_empty_songs.to_csv(OUTPUT_PATH / rows_empty_track_name )
    logging.warning("The shape of the dataframe with empty track name is {}".format(df_empty_songs.shape))
    logging.warning("^^^^ ROWS with EMPTY TRACK NAME in the input file. Those rows has been save to file '{}' ".format(rows_empty_track_name))

    ########################################################################## 
    # Get the data for the song name only from the dataframe df_songs, the df_songs is for all songs
    df_songs = df_songs.loc[df_songs["cc_track"].str.strip() != '']
    song_names = df_songs['cc_track'].unique()
    song_names = list(map(lambda x: x.strip('"').strip("'").strip().lower(), song_names)) # remove the single quote, double qote and spaces from the song_names 
    
    logging.info("There are {} unique song names in the dataframe...\n".format(len(song_names)))
    logging.debug("The list of the song names are: {}".format(', '.join(song_names)))
    logging.debug("The column names in the df_songs are:\n {} \n".format(', '.join(df_songs.columns)))

    return df_songs, song_names


def main():

    ####################################################################################
    # get the artist names and dataframe 
    df_songs, song_names = get_song_statement_data(INPUT_FILE)
    logging.info("The number of artists is {}".format(len(song_names)))

    ## Resume the process from the last_process index number if the restart parameter is provided
    if len(sys.argv) >= 2 and sys.argv[1] == 'restart':
        # read the pickle file snapshot.pkl, into the following variables
        # read the file process_song_index 
        restart_song_index = open('output/restart_song_index.txt', 'r').read()
        logging.info("The restart_song_index is {}".format(restart_song_index))
        logging.info("<<<<< The program RESTARTED from the song index {} >>>>>>".format(restart_song_index))
        start_song_index = int(restart_song_index) 
    else: 
        logging.info("<<<<< The program STARTED from the song index {} >>>>>>".format(START_SONG_INDEX))
        start_song_index = START_SONG_INDEX

    # now process the song one by one
    logging.warning("^^^^^ The song index which less than {} are skipped...\n".format(start_song_index))
    for song_index, song_name in enumerate(song_names[start_song_index: END_SONG_INDEX + 1]):
        actual_song_index = song_index + start_song_index
        logging.info("********* Processing song index: {}, the song name:  '{}',  *********".format(actual_song_index, song_name))

        # get the df_song which for this song only
        df_client_song = df_songs.loc[df_songs['cc_track'].str.strip("'").str.strip('"').str.strip().str.lower() == song_name.lower()]

        logging.info("The shape of df_song is {}".format(df_client_song.shape))
        process_one_song(actual_song_index, song_name, df_client_song)

    # TODO, do we need to merge all the pickle files to one final one? 
    logging.info("The program finished...")
    logging.debug("The function time dict is:\n {}".format(FUNC_TIME_DICT))
    for func_name, time_list in FUNC_TIME_DICT.items():
        if len(time_list) == 0: continue
        logging.info("The function {} took {}s in average".format(func_name, sum(time_list)/len(time_list)))

if __name__ == "__main__":
    main()  

    # df_songs, song_names = get_song_statement_data('cc_soave.pkl')
    # # print(song_names)
    # print(len(song_names))
    # print(df_songs.head())
    # print(df_songs.shape)
    # print(df_songs)

    
    