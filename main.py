import os, sys
import pickle
import logging
from settings import LOG_PATH, OUTPUT_PATH, START_SONG_INDEX, END_SONG_INDEX, LOG_LEVEL, INPUT_FILE, INPUT_PATH, ARTIST_ALIAS
from settings import TENCENT_COVERAGE
from one_song_process import process_one_song
from modules.common import timeit, FUNC_TIME_DICT
from pathlib import Path
import pandas as pd
from itertools import chain
# from modules.msv7 import save_to_excel_v2

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
    
    # update the cc_version na to be generic
    df_songs['cc_version'] = df_songs['cc_version'].fillna('generic')

    logging.info("There are {} unique song names in the dataframe...\n".format(len(song_names)))
    logging.debug("The list of the song names are: {}".format(', '.join(song_names)))
    logging.debug("The column names in the df_songs are:\n {} \n".format(', '.join(df_songs.columns)))

    return df_songs, song_names

# implement a dictionary like mapping, and case insensitive
def get_artist_alias(x):
    for k, v in ARTIST_ALIAS.items():
        if x.lower().strip() == k.lower().strip():
            return v
    return x


def main():
    pickle_path = Path("output/pickle")
    excel_path = Path("output/excel")

    ####################################################################################
    # archive the files in OUTPUT_PATH to OUTPUT_PATH/ "archive" folder, filename is attached with the current date
    for file in chain(OUTPUT_PATH.glob("*.xlsx"), OUTPUT_PATH.glob("*.csv")):
        os.rename(file, OUTPUT_PATH / "archive" / (file.name + '.' + str(today)))
    
    ####################################################################################
    ## Resume the process from the last_process index number if the restart parameter is provided
    if len(sys.argv) >= 2 and sys.argv[1] == 'restart':
        # read the pickle file snapshot.pkl, into the following variables
        # read the file process_song_index 
        restart_song_index = open('output/restart_song_index.txt', 'r').read()
        logging.info("The restart_song_index is {}".format(restart_song_index))
        logging.info("<<<<< The program RESTARTED from the song index {} >>>>>>".format(restart_song_index))
        start_song_index = int(restart_song_index) 
    else: 
        # get the input from the user to confirm if the pickle files and excels should be deleted or not
        # if answer is yes, proceed , if the answer is no, stop the process
        answer = input("The cached pickle files in folder {} and excel files in folder {} will be deleted. \n Please use 'main.py restart' if you want to start from the previous checkpoint...(Y/N)".format(pickle_path, excel_path))
        if answer.lower() == 'n':
            logging.warning("The process is stopped")
            exit()

        ######################################################
        # remove all the pick fills in the pickle_path and excel_path
        for file in pickle_path.glob("*.pkl"):
            os.remove(file)
        for file in excel_path.glob("*.xlsx"):
            os.remove(file)
        logging.info("All the pickle files in the folder '{}' are removed...".format(pickle_path))
        logging.info("All the execl files in the folder '{}' are removed...".format(excel_path))

        logging.info("<<<<< The program STARTED from the song index {} >>>>>>".format(START_SONG_INDEX))
        start_song_index = START_SONG_INDEX

    ####################################################################################
    # get the artist names and dataframe 
    df_songs, song_names = get_song_statement_data(INPUT_FILE)
    logging.info("The dataframe of the input client statements has {} rows".format(df_songs.shape[0]))
    
    logging.debug('\n'.join(song_names[START_SONG_INDEX: END_SONG_INDEX + 1]))

    # replace the artist_names with alias included
    df_songs["cc_artist"] = df_songs["cc_artist"].apply(get_artist_alias)

    logging.info("The cc_artist column in client statement has included the alias name ...")
    logging.info("The number of artists is {}".format(len(song_names)))
    
    ####################################################################################
    # now process the song one by one
    logging.warning("^^^^^ The song index which less than {} are skipped...\n".format(start_song_index))
    for song_index, song_name in enumerate(song_names[start_song_index: END_SONG_INDEX + 1]):
        actual_song_index = song_index + start_song_index
        logging.info("********* Processing song index: {}, the song name:  '{}',  *********".format(actual_song_index, song_name))

        # get the df_song which for this song only
        df_client_song = df_songs.loc[df_songs['cc_track'].str.strip("'").str.strip('"').str.strip().str.lower() == song_name.lower()]

        logging.info("The shape of df_song is {}".format(df_client_song.shape))
        process_one_song(actual_song_index, song_name, df_client_song)

    ######################################################
    # now get all the pickle files from output/pickle folder, read them and the merge them into final excel file in output folder
    logging.info(" ###### Final Summarization ######")

    pickle_files_details = list(pickle_path.glob("N*.pkl"))
    logging.info("The number of pickle files is {}".format(len(pickle_files_details)))

    dfs_matched, dfs_unmatched  = pd.DataFrame(), pd.DataFrame() 
    for file in pickle_files_details:
        matched_df, unmatched_df = pickle.load(open(file, 'rb'))
        logging.info("The matched df shape is {}, unmatched df shape is {}, filename is {}".format(matched_df.shape, unmatched_df.shape, file.name))  
        dfs_matched = pd.concat([dfs_matched, matched_df])
        dfs_unmatched = pd.concat([dfs_unmatched, unmatched_df])

    pickle_files_summary = list(pickle_path.glob("summary*.pkl"))
    logging.info("The number of summary pickle files is {}".format(len(pickle_files_summary)))

    dfs_summary = pd.DataFrame()
    for file in pickle_files_summary:
        df_summary = pickle.load(open(file, 'rb'))
        logging.info("The summary df shape is {}".format(df_summary.shape))  
        dfs_summary = pd.concat([dfs_summary, df_summary])

    logging.info("The final matched df shape is {}, unmatched df shape is {}".format(dfs_matched.shape, dfs_unmatched.shape))
    logging.info("the final summary df shape is {}".format(dfs_summary.shape))

    # save_to_excel_v2_matched(dfs_matched, dfs_unmatched, OUTPUT_PATH / "final_result.xlsx")
    # output all the result to the final excel file and csv file
    dfs_matched.reset_index(drop=True).to_excel(OUTPUT_PATH / "matched_result_details.xlsx", index=True)
    dfs_unmatched.reset_index(drop=True).to_csv(OUTPUT_PATH/ "unmatched_result.csv", index=False)
    logging.debug(dfs_summary.columns)

    # special logic to calculate the total comment for those tencent related platform
    dfs_summary[('tencent', 'tencent total comments')] = dfs_summary.apply(
        lambda row: sum( [row[('tencent', '{} p_comments'.format(p))] 
             for p in TENCENT_COVERAGE]), 
             axis=1)

    dfs_summary_final = dfs_summary.reset_index(drop=True).sort_index(level=[0,1], axis=1)
    dfs_summary_final.columns = [col[1] for col in dfs_summary_final.columns.values] # only keep the level 1 index
    dfs_summary_final['matched count'] = dfs_summary_final['matched count'].fillna(0).astype(int)
    dfs_summary_final.to_excel(OUTPUT_PATH / "matched_result_summary.xlsx", index=True)
    # the save the matched result and unmatched result to csv files
    # dfs_matched.to_csv(OUTPUT_PATH / "matched_result.csv")
    # dfs_unmatched.to_csv(OUTPUT_PATH / "unmatched_result.csv")

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

    
    