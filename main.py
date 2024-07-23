import os, sys
import pickle
import logging
from settings import LOG_PATH, OUTPUT_PATH, START_ARTIST_INDEX, END_ARTIST_INDEX, ARTIST_NAMES, LOG_LEVEL
from one_artist_process import process_one_artist


log_name = LOG_PATH / 'artist_process.log'
# setup the logging to output the log to console and log file. 
logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    handlers=[logging.FileHandler(log_name, mode='a'),
                              logging.StreamHandler()
                              ]
                    )


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
                # TODO the cc_twosteps is hardcoded, need to be modified
                # TODO the cc_twosteps is hardcoded, need to be modified
                process_one_artist(artist_seq_no, artist_name, "cc_twosteps.pkl", restart_snapshot = restart_snapshot )

    else: 
        logging.info("<<<<< The program STARTED >>>>>>")
        for artist_seq_no, artist_name in enumerate(ARTIST_NAMES):
            logging.info("********* Processing the artist '{}', artist seq no is {} *********".format(artist_name, artist_seq_no))
            if artist_seq_no < START_ARTIST_INDEX or artist_seq_no > END_ARTIST_INDEX:
                logging.warning("The artist seq no {seq_no} is out of the range [{start}, {end}], skipped...".format(seq_no=artist_seq_no, start=START_ARTIST_INDEX, end=END_ARTIST_INDEX))
            else: 
                # TODO the cc_twosteps is hardcoded, need to be modified
                # TODO the cc_twosteps is hardcoded, need to be modified
                process_one_artist(artist_seq_no, artist_name, "cc_twosteps.pkl" )

if __name__ == "__main__":
    main()  
    