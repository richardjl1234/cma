import os, sys
import pickle
import logging
from settings import LOG_PATH, OUTPUT_PATH, START_SONG_INDEX, END_SONG_INDEX, LOG_LEVEL, INPUT_FILE, INPUT_PATH, ARTIST_ALIAS
from settings import  PLATFORM_IN_SCOPE_CLIENT_STATEMENT, PLATFORM_NAME_MAPPING_DICT, PLATFORMS_DB_IN_SCOPE
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

    ## for milestone5, patch to address the null value in cc_artist and c_album
    df_songs['cc_artist'] = df_songs['cc_artist'].astype(str)
    df_songs['c_album'] = df_songs['c_album'].astype(str)

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

# this function for All match sheet (present to client)
def create_final_details_client_df(df_matched_detail: pd.DataFrame, df_client_summary: pd.DataFrame) -> pd.DataFrame: 
    logging.debug(f'{df_client_summary.columns =}')
    df_input = df_matched_detail.merge(df_client_summary, on=['cc_track', 'cc_version'])
    logging.debug(f'{df_input.columns =}')

    df_output = pd.DataFrame()
    # df_output[('Catalog Metadata ', '#')] = df_matched_detail.index
    df_output[('Catalog Metadata ', 'Unique Song ID')] = df_input['Unique Song ID']
    df_output[('Catalog Metadata ', 'Unique Version ID')] = df_input['Unique Version ID']
    df_output[('Catalog Metadata ', 'Reference Track')] = df_input['c_track']
    df_output[('Catalog Metadata ', 'Ref. Artist')] = df_input['c_artist']
    df_output[('Catalog Metadata ', 'Album Name')] = df_input['c_album']
    # platform 
    # df_output[('Catalog Metadata ', 'Platform')] = df_input['p_platform'].map({'netease_max': 'NetEase', 'kugou': "Kugou", 'qqmusicv2': 'QQMusic'})
    df_output[('Catalog Metadata ', 'Platform')] = df_input['p_platform'].map(PLATFORM_NAME_MAPPING_DICT)

    # TODO how to remove those hardcoding
    # revenue
    df_output[('Catalog Metadata ', 'Total Revenue')] = df_input.apply(
        lambda row: row[('Catalog Overview', '{} Total Revenue'.format(PLATFORMS_DB_IN_SCOPE.get(row['p_platform'])))]
        if row['p_platform'] in PLATFORMS_DB_IN_SCOPE else 'NA'
        ,axis=1)

        # TODO how to remove those hardcoding
        # Streams
    df_output[('Catalog Metadata ', 'Total Streams')] = df_input.apply(
        lambda row: row[('Catalog Overview', '{} Total Streams'.format(PLATFORMS_DB_IN_SCOPE.get(row['p_platform'])))]
        if row['p_platform'] in PLATFORMS_DB_IN_SCOPE else 'NA'
        ,axis=1)

    df_output[('Matched Data', 'Artist Name')] = df_input['pc_artist'] # ok
    df_output[('Matched Data', 'Track Title')] = df_input['p_track']
    df_output[('Matched Data', 'Album Name')] = df_input['p_album'] # ok
    df_output[('Matched Data', 'Platform')] = df_input['p_platform'] # ok
    df_output[('Matched Data', 'Release Date')] = df_input['p_release_date']
    df_output[('Matched Data', 'Licensor (Company)')] = df_input['p_company']
    df_output[('Matched Data', 'Comments')] = df_input['p_comments'] # ok
    df_output[('Matched Data', 'Favorites')] = df_input['p_likes_count'] # ok
    df_output[('Matched Data', 'Estimated Streams')] =  '' 
    df_output[('Matched Data', 'Link')] = ''

    # claimed or not
    df_output[('Track Status', 'Claim Status ')] = df_input.apply(
        lambda row: 
        'claimed' if row['pc_version'] == row['cc_version'] and row['refine_platform_match'] == 'Y' and row['refine_similarity'] == 1
        else 'unclaimed'
        ,axis=1)

    df_output[('Track Status', 'Other')] = ''
    df_output[('Track Status', 'Notes')] = ''
    df_output.columns = pd.MultiIndex.from_tuples(df_output.columns)

    return df_output


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
    # df_songs["cc_artist"] = df_songs["cc_artist"].apply(get_artist_alias)

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
        # add the alias name for the artist
        df_client_song["cc_artist"] = df_client_song["cc_artist"].apply(get_artist_alias)

        logging.info("The shape of df_song is {}".format(df_client_song.shape))
        process_one_song(actual_song_index, song_name, df_client_song)

    ######################################################
    # now get all the pickle files from output/pickle folder, read them and the merge them into final excel file in output folder
    logging.info(" ###### Final Summarization ######")

    pickle_files_details = list(pickle_path.glob("N*-matched_internal.pkl"))
    logging.info("The number of pickle files is {}".format(len(pickle_files_details)))

    # matched detail dataframe and unmatched datafarame, retrieve from pickle file
    dfs_final_details_internal, dfs_unmatched  = pd.DataFrame(), pd.DataFrame() 
    for file in pickle_files_details:
        matched_df, unmatched_df = pickle.load(open(file, 'rb'))
        logging.info("The matched df shape is {}, unmatched df shape is {}, filename is {}".format(matched_df.shape, unmatched_df.shape, file.name))  
        dfs_final_details_internal = pd.concat([dfs_final_details_internal, matched_df])
        dfs_unmatched = pd.concat([dfs_unmatched, unmatched_df])
    logging.info("The final matched df shape is {}, unmatched df shape is {}".format(dfs_final_details_internal.shape, dfs_unmatched.shape))

    # matched summary dataframe for CLIENT
    pickle_files_summary = list(pickle_path.glob("summary_client*.pkl"))
    logging.info("The number of summary (client) pickle files is {}".format(len(pickle_files_summary)))

    dfs_final_summary_client = pd.DataFrame()
    for file in pickle_files_summary:
        df_summary = pickle.load(open(file, 'rb'))
        logging.info("The summary (client) df shape is {}".format(df_summary.shape))  
        dfs_final_summary_client = pd.concat([dfs_final_summary_client, df_summary])
    logging.info("the final summary (client) df shape is {}".format(dfs_final_summary_client.shape))

    # matched summary dataframe for INTERNAL Usage
    # pickle_files_summary_internal = list(pickle_path.glob("summary_internal*.pkl"))
    # logging.info("The number of summary (internal) pickle files is {}".format(len(pickle_files_summary_internal)))

    # dfs_final_summary_internal = pd.DataFrame()
    # for file in pickle_files_summary_internal:
    #     df_summary_internal = pickle.load(open(file, 'rb'))
    #     logging.info("The summary (internal) df shape is {}".format(df_summary_internal.shape))  
    #     dfs_final_summary_internal= pd.concat([dfs_final_summary_internal, df_summary_internal])
    # logging.info("the final summary (internal) df shape is {}".format(dfs_final_summary_internal.shape))
    ## TODO to fix the issues 
    # special logic to calculate the total comment for those tencent related platform
    # dfs_summary[('tencent', 'tencent total comments')] = dfs_summary.apply(
    #     lambda row: sum(summary_clientsummary_client [row[('tencent', '{} p_comments'.format(p))] 
    #          for p in TENCENT_COVERAGE]), 
    #          axis=1)

    # dfs_client_platform_merged = dfs_summary.reset_index(drop=True).sort_index(level=[0,1], axis=1)
    # dfs_final_summary_client = dfs_final_summary_internal
    # dfs_client_platform_merged.columns = [col[1] for col in dfs_client_platform_merged.columns.values] # only keep the level 1 index
    logging.info("The columns of final summary dataframe is {}".format(dfs_final_summary_client.columns))
    # dfs_client_platform_merged.columns = [col.strip() for col in dfs_client_platform_merged.columns]
    dfs_final_summary_client[('Catalog Overview', 'Total Matches Detected')] = dfs_final_summary_client[
        ('Catalog Overview', 'Total Matches Detected')].fillna(0).astype(int)
    
    # before dfs_summary_final save to disk, merge the data with the df_songs (the client statement)
    # only get the result for inscope data

    df_songs.set_index(['cc_track', 'cc_version'], inplace=True)
    artist_dict = df_songs['c_artist'].to_dict()

    track_title_dict = df_songs['c_track'].to_dict()
    unique_song_id_dict = df_songs['Unique Song ID'].to_dict()

    dfs_final_summary_client.set_index(['cc_track', 'cc_version'], inplace=True)
    # merget the df_songs_summary and dfs_client_platform_merged into final excel file
    dfs_final_summary_client[('Catalog Metadata', 'Artist Name')] = dfs_final_summary_client.index.map(artist_dict)
    dfs_final_summary_client[('Catalog Metadata', 'Track Title')] = dfs_final_summary_client.index.map(track_title_dict)
    dfs_final_summary_client[('Catalog Metadata', 'Unique Song ID')] = dfs_final_summary_client.index.map(unique_song_id_dict)

    # create the dfs_final_summary_internal dataframe from the dfs_final_summary_client dataframe
    dfs_final_summary_client.reset_index(drop=False, inplace=True, names=['cc_track', 'cc_version'])
    dfs_final_summary_internal = create_summary_internal_df(dfs_final_summary_client)

    dfs_final_details_internal = dfs_final_details_internal.reset_index(drop=True)
    # milestone 5
    dfs_final_details_internal = dfs_final_details_internal.drop_duplicates()
    
    # dfs_final_details_internal.to_excel(OUTPUT_PATH/"excel"/'temp1.xlsx')
    # dfs_final_summary_client.to_excel(OUTPUT_PATH/'excel'/'temp2.xlsx')

    dfs_final_details_client = create_final_details_client_df(dfs_final_details_internal, dfs_final_summary_client)

    dfs_final_summary_client.drop(columns=['cc_track', 'cc_version'], inplace=True)

    dfs_final_summary_client.columns = pd.MultiIndex.from_tuples(dfs_final_summary_client.columns)
    dfs_final_summary_client.sort_index(level=[0, 1], axis=1, inplace=True)
    # for cols in dfs_client_platform_merged.columns:
    #     print(cols)

    # dfs_final_summary_client.to_excel(OUTPUT_PATH/ 'excel' / 'temp112.xlsx')

    dfs_final_summary_client = reorg_final_result(dfs_final_summary_client)

    # dfs_client_platform_merged.to_excel(OUTPUT_PATH / "matched_result_summary.xlsx", index=True)

    logging.info("💾 Saving matched summary and detail results to Excel...")
    final_result_name = OUTPUT_PATH / "matched_result_final.xlsx"
    with pd.ExcelWriter(final_result_name, engine='openpyxl') as writer:
       # Save matched data to client summary
       dfs_final_summary_client.to_excel(writer, sheet_name='Catalogue overview', index=True)
       dfs_final_details_client.to_excel(writer, sheet_name='All Matches', index=True)
    
       # save to detail and summary to internal sheets
       dfs_final_summary_internal.to_excel(writer, sheet_name='Catalogue overview (internal)', index=True)

    #    dfs_final_details_internal.drop(columns=['c_platform'], inplace=True)
       dfs_final_details_internal.reset_index(drop=True).to_excel(writer, sheet_name='All Matches (internal)', index=True)
    
    logging.info("📊 Matched summary and details are saved to Excel file {}. ".format(str(final_result_name)) )
    # save the unmatched data to csv file
    dfs_unmatched.reset_index(drop=True).to_csv(OUTPUT_PATH/ "unmatched_result.csv", index=True)
    # the save the matched result and unmatched result to csv files
    # dfs_matched.to_csv(OUTPUT_PATH / "matched_result.csv")
    # dfs_unmatched.to_csv(OUTPUT_PATH / "unmatched_result.csv")

    logging.info("The program finished...")
    logging.debug("The function time dict is:\n {}".format(FUNC_TIME_DICT))
    for func_name, time_list in FUNC_TIME_DICT.items():
        if len(time_list) == 0: continue
        logging.info("The function {} took {}s in average".format(func_name, sum(time_list)/len(time_list)))

def reorg_final_result(df: pd.DataFrame):

    logging.info("reorg the final summary result....")

    df[('Catalog Overview', 'Estimated Revenue')] = ''
    df[('Catalog Overview', 'Estimated Streams')] = ''
    # df[('Catalog Overview', 'Total Favorites')] = ''

    if 'NetEase Cloud Music' in PLATFORM_NAME_MAPPING_DICT.values():  
        df[('NetEase Cloud Music', 'Total Matches')] = df[('NetEase Cloud Music', 'Claimed Matches')] + df[('NetEase Cloud Music', 'Unclaimed Matches')]
        # df[('NetEase Cloud Music', 'Favorites (Claimed)')] =  ''
        # df[('NetEase Cloud Music', 'Favorites (Unclaimed)')] =  ''
        df[('NetEase Cloud Music', 'Revenue')] =  df[('Catalog Overview', 'NetEase Total Revenue')]

    if 'Kugou Music' in PLATFORM_NAME_MAPPING_DICT.values():  
        df[('Kugou Music', 'Total Matches')] = df[('Kugou Music', 'Claimed Matches')] + df[('Kugou Music', 'Unclaimed Matches')]
        # df[('Kugou Music', 'Favorites (Claimed)')] =  ''
        # df[('Kugou Music', 'Favorites (Unclaimed)')] =  ''
        df[('Kugou Music', 'Revenue')] =  df[('Catalog Overview', 'Kugou Music Total Revenue')]

    if 'QQ Music' in PLATFORM_NAME_MAPPING_DICT.values():  
        df[('QQ Music', 'Total Matches')] = df[('QQ Music', 'Claimed Matches')] + df[('QQ Music', 'Unclaimed Matches')]
        # df[('QQ Music', 'Favorites (Claimed)')] =  ''
        # df[('QQ Music', 'Favorites (Unclaimed)')] =  ''
        df[('QQ Music', 'Revenue')] =  df[('Catalog Overview', 'QQ Music Total Revenue')]
    columns = [
        ('Catalog Metadata', 'Unique Song ID'), 
        ('Catalog Metadata', 'Artist Name'), 
        ('Catalog Metadata', 'Track Title'),
        ('Catalog Metadata', 'Total Revenue'), 
        ('Catalog Metadata', 'Total Streams'), 
        ('Catalog Overview', 'Total Matches Detected'), 
        ('Catalog Overview', 'Estimated Revenue'), 
        ('Catalog Overview', 'Estimated Streams'), 
        # ('Catalog Overview', 'NetEase Total Revenue'), 
        # ('Catalog Overview', 'NetEase Total Streams'), 
        # ('Catalog Overview', 'Tencent Total Streams'), 
        ('Catalog Overview', 'Total Comments'), 
        ('Catalog Overview', 'Total Favorites'), 
        ('Catalog Overview', 'Tencent Total Revenue'),
    ]

    if 'NetEase Cloud Music' in PLATFORM_NAME_MAPPING_DICT.values():  
        columns += [
        ('NetEase Cloud Music', 'Total Matches'), 
        ('NetEase Cloud Music', 'Claimed Matches'), 
        ('NetEase Cloud Music', 'Unclaimed Matches'), 
        ('NetEase Cloud Music', 'Revenue'),
        ('NetEase Cloud Music', 'Comments (Claimed)'), 
        ('NetEase Cloud Music', 'Favorites (Claimed)'), 
        ('NetEase Cloud Music', 'Comments (Unclaimed)'), 
        ('NetEase Cloud Music', 'Favorites (Unclaimed)'), 
        ]
        # ('NetEase Cloud Music', 'Likes (Claimed)'), 
        # ('NetEase Cloud Music', 'Likes (Unclaimed)'), 
        # ('NetEase Cloud Music', 'Streams (Claimed)'), 
        # ('NetEase Cloud Music', 'Streams (Unclaimed)'), 

    if 'Kugou Music' in PLATFORM_NAME_MAPPING_DICT.values():  
        columns += [
            ('Kugou Music', 'Total Matches'), 
            ('Kugou Music', 'Claimed Matches'), 
            ('Kugou Music', 'Unclaimed Matches'), 
            ('Kugou Music', 'Revenue'),
            ('Kugou Music', 'Comments (Claimed)'), 
            ('Kugou Music', 'Favorites (Claimed)'), 
            ('Kugou Music', 'Comments (Unclaimed)'), 
            ('Kugou Music', 'Favorites (Unclaimed)'), 
            # ('Kugou Music', 'Claimed Matches'), 
            # ('Kugou Music', 'Comments (Claimed)'), 
            # ('Kugou Music', 'Comments (Unclaimed)'), 
            # ('Kugou Music', 'Likes (Claimed)'), 
            # ('Kugou Music', 'Likes (Unclaimed)'), 
            # ('Kugou Music', 'Streams (Claimed)'), 
            # ('Kugou Music', 'Streams (Unclaimed)'), 
            # ('Kugou Music', 'Unclaimed Matches'), 
        ]

    if 'QQ Music' in PLATFORM_NAME_MAPPING_DICT.values():  
        columns += [
            ('QQ Music', 'Total Matches'), 
            ('QQ Music', 'Claimed Matches'), 
            ('QQ Music', 'Unclaimed Matches'), 
            ('QQ Music', 'Revenue'),
            ('QQ Music', 'Comments (Claimed)'), 
            ('QQ Music', 'Favorites (Claimed)'), 
            ('QQ Music', 'Comments (Unclaimed)'), 
            ('QQ Music', 'Favorites (Unclaimed)'), 
            # ('QQ Music', 'Claimed Matches'), 
            # ('QQ Music', 'Comments (Claimed)'), 
            # ('QQ Music', 'Comments (Unclaimed)'), 
            # ('QQ Music', 'Likes (Claimed)'), 
            # ('QQ Music', 'Likes (Unclaimed)'), 
            # ('QQ Music', 'Streams (Claimed)'), 
            # ('QQ Music', 'Streams (Unclaimed)'), 
            # ('QQ Music', 'Unclaimed Matches'), 
        ]
    
    df=df.loc[:,pd.MultiIndex.from_tuples(columns)] 

    return df

# for Catalogue overview (internal) sheet
def create_summary_internal_df(df_summary_client: pd.DataFrame):
    df_summary_internal = pd.DataFrame() 

    df_summary_internal['Unique Song ID'] = df_summary_client[('Catalog Metadata', 'Unique Song ID')]
    df_summary_internal['matched count'] = df_summary_client[('Catalog Overview', 'Total Matches Detected')]
    df_summary_internal['Total Comments'] = df_summary_client[('Catalog Overview', 'Total Comments')]
    df_summary_internal['Total Favorites'] = df_summary_client[('Catalog Overview', 'Total Favorites')]
    df_summary_internal['Total Revenue'] = df_summary_client[('Catalog Metadata', 'Total Revenue')]
    df_summary_internal['cc_track'] = df_summary_client['cc_track']
    df_summary_internal['cc_version'] = df_summary_client['cc_version']

    # change the 'Total Revenue' from the client statement to c_revenue
    for col in df_summary_client.columns: 
        if 'Total Revenue' in col[1] and col[0] == 'Catalog Overview': 
            col_name = col[1].removesuffix(' Total Revenue') + ' c_revenue'
            df_summary_internal[col_name] = df_summary_client[col]

    # platform reverse mapping
    platform_reverse_dict = {}
    for k, v in PLATFORM_NAME_MAPPING_DICT.items(): 
        platform_reverse_dict[v] = k

    # column name reverse mapping
    p_column_mapping_dict = {
        'Comments (Claimed)': 'p_comments (Claimed)',
        'Comments (Unclaimed)': 'p_comments (Unclaimed)', 
        'Favorites (Claimed)': 'p_likes_count (Claimed)',
        'Favorites (Unclaimed)': 'p_likes_count (Unclaimed)', 
        'Streams (Claimed)': 'p_streams (Claimed)',
        'Streams (Unclaimed)': 'p_streams (Unclaimed)', 
    }

    for k, v  in platform_reverse_dict.items(): 
        for col in df_summary_client.columns: 
            if k == col[0] and col[1] in p_column_mapping_dict: 
                col_name = v + ' ' + p_column_mapping_dict[col[1]]
                df_summary_internal[col_name] = df_summary_client[col]

    logging.debug(f"{df_summary_internal.columns = }")
    return df_summary_internal

if __name__ == "__main__":
    main()  

    # df_songs, song_names = get_song_statement_data('cc_soave.pkl')
    # # print(song_names)
    # print(len(song_names))
    # print(df_songs.head())
    # print(df_songs.shape)
    # print(df_songs)

    
    