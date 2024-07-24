import os, sys
import pickle
import logging
from settings import LOG_PATH, OUTPUT_PATH, START_ARTIST_INDEX, END_ARTIST_INDEX, LOG_LEVEL, INPUT_FILE, INPUT_PATH
from one_artist_process import process_one_artist
from modules.common import timeit
from pathlib import Path
import pandas as pd


log_name = LOG_PATH / 'artist_process.log'
# setup the logging to output the log to console and log file. 
logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    handlers=[logging.FileHandler(log_name, mode='a'),
                              logging.StreamHandler()
                              ]
                    )

@timeit
def get_artist_statement(client_statement_file, input_folder= INPUT_PATH)   :
    ########################################################################## 
    # read the file_name into data frame
    logging.info("Read the input file '{}' into memory...".format(client_statement_file))
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
    artists = df_artists['c_artist'].unique()
    logging.info("There are {} rows in the dataframe, the list of the artist name are: {}".format(len(artists), ', '.join(artists)))
    logging.debug("The column names in the df_singer are:\n {} \n".format(', '.join(df_artists.columns)))

    ########################################################################## 
    # for those rows which has blank track name, output the report in the output folder
    # get the disctinct song_name from the df_singer.cc_track column, save to song_names, 
    rows_empty_track_name = "{}.csv".format("EMPTY_TRACK_NAME")
    df_artists.loc[df_artists["cc_track"] == ''].to_csv(OUTPUT_PATH / rows_empty_track_name )
    logging.warning("^^^^ ROWS with EMPTY TRACK NAME in the input file. Those rows has been save to file '{}' ".format(rows_empty_track_name))

    return df_artists, artists


def main():

    ####################################################################################
    # get the artist names and dataframe 
    df_artists, artist_names = get_artist_statement(INPUT_FILE)
    logging.info("The number of artists is {}".format(len(artist_names)))

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

        for artist_seq_no, artist_name in enumerate(artist_names):
            logging.info("********* Processing the artist '{}', artist seq no is {} *********".format(artist_name, artist_seq_no))
            if artist_seq_no < restart_artist_seq_no or artist_seq_no > END_ARTIST_INDEX:
                logging.warning("The artist seq no {seq_no} is out of the range [{start}, {end}], skipped...".format(seq_no=artist_seq_no, start=START_ARTIST_INDEX, end=END_ARTIST_INDEX))
            else: 
                df_singer = df_artists.loc[df_artists['c_artist'] == artist_name]
                logging.info("The shape of df_singer is {}".format(df_singer.shape))
                process_one_artist(artist_seq_no, artist_name, df_singer, restart_snapshot = restart_snapshot )

    else: 
        logging.info("<<<<< The program STARTED >>>>>>")
        for artist_seq_no, artist_name in enumerate(artist_names):
            logging.info("********* Processing the artist '{}', artist seq no is {} *********".format(artist_name, artist_seq_no))
            if artist_seq_no < START_ARTIST_INDEX or artist_seq_no > END_ARTIST_INDEX:
                logging.warning("The artist seq no {seq_no} is out of the range [{start}, {end}], skipped...".format(seq_no=artist_seq_no, start=START_ARTIST_INDEX, end=END_ARTIST_INDEX))
            else: 
                df_singer = df_artists.loc[df_artists['c_artist'] == artist_name]
                logging.info("The shape of df_singer is {}".format(df_singer.shape))
                process_one_artist(artist_seq_no, artist_name, df_singer)

if __name__ == "__main__":
    main()  
    