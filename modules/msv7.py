import pandas as pd
from fuzzywuzzy import fuzz
import logging
import sys, os
import pickle

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from settings import   LOG_LEVEL, VERSION_ALIAS, PLATFORM_COLS, CLIENT_COLS
from modules.common import timeit



def extract_columns(excel_path, csv_path):
   """
   Extract specific columns from Excel and CSV files.
   """
   logging.info("📂 Extracting data from files...")
   client_df = pd.read_excel(excel_path, usecols=CLIENT_COLS)
   platform_df = pd.read_csv(csv_path, usecols=PLATFORM_COLS)
   logging.info("Data extracted.")
   return client_df, platform_df


def preprocess_data(client_df, platform_df):
   """
   Add a preprocessing step to clean the data appropriately.
   """
   logging.info("🔄 Preprocessing data...")


   # Lower the cases and then remove the leading + trailing whitespaces (to standardize the data)
   client_df['cc_version'] = client_df['cc_version'].fillna('generic').str.lower().str.strip()
   platform_df['pc_version'] = platform_df['pc_version'].fillna('generic').str.lower().str.strip()


   client_df['cc_track'] = client_df['cc_track'].str.lower().str.strip()
   platform_df['pc_track'] = platform_df['pc_track'].str.lower().str.strip()


   # Aggregate the data to handle possible duplicates and reset the index
   client_df = client_df.groupby(['cc_track', 'cc_version', 'cc_artist', 'c_album']).first().reset_index()
   logging.info("Data preprocessed.")


   return client_df, platform_df

@timeit
def match_tracks(client_df, platform_df):
   """
   Match the tracks from the 'platform' dataframe to the 'client' dataframe.
   """
   logging.info("🔍 Matching tracks...")


   matched = []
   unmatched = []
  
   unmatched_tracks = {track: True for track in platform_df['pc_track'].unique()}


   for index, p_row in platform_df.iterrows():
       potential_matches = client_df[client_df['cc_track'] == p_row['pc_track']]


       if not potential_matches.empty:
           best_match = potential_matches.iloc[0] 


           for _, c_row in potential_matches.iterrows():
               if c_row['cc_version'] == p_row['pc_version']:
                   best_match = c_row
                   break


           combined_data = {**p_row.to_dict(), **best_match.to_dict(), 'match_id': index + 1}
           matched.append(combined_data)
           unmatched_tracks[p_row['pc_track']] = False
       else:
           unmatched.append(p_row.to_dict())


       if index % 100 == 0:
           logging.info(f"Processed {index + 1} tracks...")


   for track in unmatched_tracks:
       if unmatched_tracks[track]:
           unmatched_rows = platform_df[platform_df['pc_track'] == track]
           for index, row in unmatched_rows.iterrows():
               best_match_score = 0
               best_match_index = None


               for c_index, c_row in client_df.iterrows():
                   match_score = fuzz.ratio(row['pc_track'], c_row['cc_track'])
                   if match_score > best_match_score:
                       best_match_score = match_score
                       best_match_index = c_index


               if best_match_index is not None:
                   best_match_row = client_df.iloc[best_match_index]
                   combined_data = {**row.to_dict(), **best_match_row.to_dict(), 'match_id': index + 1}
                   matched.append(combined_data)
                   unmatched_tracks[track] = False
                   break  # Found a match, no need to continue searching
           if unmatched_tracks[track]:
               unmatched.extend(unmatched_rows.to_dict('records'))


   logging.info("===== Matching completed.")
   return pd.DataFrame(matched), pd.DataFrame(unmatched)


def save_to_excel(matched_df, unmatched_df, output_path):
   """
   Save the results to the 'matched_results.xlsx' workbook.
   """
   logging.info("💾 Saving results to Excel...")
   with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
       # Save matched data to 'Matched' sheet
       matched_df.to_excel(writer, sheet_name='Matched', index=False)
      
       # Save unmatched data to 'Matched' sheet starting from row after matched data
       unmatched_df.to_excel(writer, sheet_name='Matched', startrow=matched_df.shape[0]+2, index=False)
      
   logging.info("📊 Data saved to Excel. Check the output file at: {}".format(str(output_path)) )

def save_result_to_excel_or_pickle(matched_df, unmatched_df, output_excel_path, output_pickle_path = None):
   """
   Save the results to the 'matched_results.xlsx' workbook.
   """
   logging.info("💾 Saving results to Excel...")
   with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
       # Save matched data to 'Matched' sheet
       matched_df.to_excel(writer, sheet_name='matched', index=False)
       # Save unmatched data to 'Matched' sheet starting from row after matched data
       unmatched_df.to_excel(writer, sheet_name='unmatched',  index=False)

   logging.info("📊 Data saved to Excel. Check the output file at: {}".format(str(output_excel_path)) )
   if output_pickle_path is not None:
      with open(output_pickle_path, 'wb') as f:
          pickle.dump((matched_df, unmatched_df),  f)
      logging.info("📊 Data saved to Pickle file. Check the output file at: {}".format(str(output_pickle_path)) )

def run_matching_operation():
   """
   Run the entire matching operation from start to finish.
   """
   base_path = '/Users/CallumAmor/Dropbox/My Mac (Cal.fios-router.home)/Desktop'
   excel_path = f'{base_path}/cc_twostepsv7.xlsx'
   csv_path = f'{base_path}/pc_twosteps.csv'
   output_path = f'{base_path}/twosteps_matchedV13232424.xlsx'
   client_df, platform_df = extract_columns(excel_path, csv_path)
   client_df, platform_df = preprocess_data(client_df, platform_df)
   matched_df, unmatched_df = match_tracks(client_df, platform_df)
   save_to_excel(matched_df, unmatched_df, output_path)

##################################################################################################################
# check if the platform name match with each other
def filter_platform_old(row_p, c_platform):
    # return True
    a =   ('netease' in row_p['p_platform'].lower().strip() and 'netease' in c_platform.lower().strip())
    b =   ('kugou' in row_p['p_platform'].lower().strip() and 'kugou' in c_platform.lower().strip())
    # logging.info(f"THE PLATFORM CHECK.... {b = }, {row_p['p_platform'] =}, {c_platform =}"  )
    c =   ('qqmusic' in row_p['p_platform'].lower().strip() and 'qqmusic' in c_platform.lower().strip())
    d =   ('kuwo' in row_p['p_platform'].lower().strip() and 'kuwo' in c_platform.lower().strip())
    return a or b or c or d

def filter_platform(row_p, c_platform):
    return True

def filter_song_name(row, song_name ):
    return row['pc_track'].lower().strip().strip('"').strip("'") == song_name.lower().strip().strip('"').strip("'")

# Define the filters below which need to be used in the refine process
# pc_artist could a list of artist which are separated by ,
def filter_artist_names(row, artist_names_c):
    # Replace the char '、' to ',' so that we can split by comma
    artist_names_in_pc_artist_column = row['pc_artist'].replace('、', ',').replace('|', ',').split(',') 
    artist_names_in_cc_artist_column = artist_names_c.replace('、', ',').replace('|', ',').split(',')

    result = any(name.lower().strip() == pc_artist_name.lower().strip()
               for name in artist_names_in_cc_artist_column 
               for pc_artist_name in artist_names_in_pc_artist_column) 
    # logging.debug(f"{result = }")
    # logging.debug(f"{artist_names_in_pc_artist_column = }")
    # logging.debug(f"{artist_names_in_cc_artist_column = }")
    return result

# define the filter that track name 
def filter_album_name(row, album_name):
    return album_name.lower().strip() == row['p_album'].lower().strip() 

# define the filter for song_versions
def filter_version(row, song_version, song_versions): # song_versions are the all versions in the client statements
    p_song_versions = row['pc_version'].replace('、', ',').replace('|', ',').split(',') 
    p_song_versions = [VERSION_ALIAS.get(version.strip(), version)   for version in p_song_versions] # replace the alias in version list
    # a = song_version.lower().strip() == row['pc_version'].lower().strip()
    a = any(song_version.lower().strip() == p_song_version.strip() for p_song_version in p_song_versions)
    # b = (song_version.lower().strip() == 'generic' and row['pc_version'].lower().strip() not in song_versions)
    b = (song_version.lower().strip() == 'generic' and all(p_song_version.strip() not in song_versions for p_song_version in p_song_versions))
    return a or b

def filter_level1(row, song_name, song_version, artist_names, album_name, platform_name, song_versions): 
    a = filter_platform(row, platform_name)
    b = filter_song_name(row, song_name)
    c = filter_version(row, song_version, song_versions)
    d = filter_artist_names(row, artist_names)
    e = filter_album_name(row, album_name)
    return a and b and c and d and e 

def filter_level2(row, song_name, song_version, artist_names, album_name, platform_name, song_versions): 
    a = filter_platform(row, platform_name)
    b = filter_song_name(row, song_name)
    c = filter_version(row, song_version, song_versions)
    d = filter_artist_names(row, artist_names)
    e = (not filter_album_name(row, album_name))
    return a and b and c and d and e 
        
def filter_level3(row, song_name, song_version, artist_names, album_name, platform_name, song_versions): 
    a = filter_platform(row, platform_name)
    b = filter_song_name(row, song_name)
    c = filter_artist_names(row, artist_names)
    return a and b and c 


@timeit
def match_tracks_v2(df_client, df_platform):

   logging.info("🔍 Matching tracks...")
   df_client['c_album'] = df_client['c_album'].fillna('')  
   df_platform['p_album'] = df_platform['p_album'].fillna('')  

   match_results = []
   unmatch_results = []
   # output the contents of df_platform and df_client in logging.debug
   logging.debug(f"{df_platform.shape[0] = } in platform data...")
   logging.debug(f"{df_client.shape[0] = } in client statements...")
   # output the unique song id from the df_platform
   song_ids = tuple(df_platform['p_song_id'].unique().tolist())
   logging.debug( f"{len(song_ids) = } song ids in platform data...")

   song_versions = tuple(df_client['cc_version'].str.lower().str.strip().unique().tolist())
   logging.info(f"{song_versions = } in client statements...")

   for idx_p, row_p in df_platform.iterrows():
       matched_level = 4
       for _, row_c in df_client.iterrows():
           if filter_level1(row_p, row_c['cc_track'], row_c['cc_version'], row_c['cc_artist'], row_c['c_album'], row_c['c_platform'], song_versions):
               row_p['refine_process_comment'] = 'Track, version, artist, album Exact Match'
               row_p['refine_similarity'] = 1
               best_match = {**row_p.to_dict(), **row_c.to_dict(), 'match_id': idx_p + 1}
               matched_level = 1
           elif filter_level2(row_p, row_c['cc_track'], row_c['cc_version'], row_c['cc_artist'], row_c['c_album'], row_c['c_platform'], song_versions): 
               if matched_level > 2:
                   row_p['refine_process_comment'] = 'Track, Version, Artist Exact Match (album not match)'
                   row_p['refine_similarity'] = 2
                   best_match = {**row_p.to_dict(), **row_c.to_dict(), 'match_id': idx_p + 1}
                   matched_level = 2
               else: 
                   logging.debug(f"there is a better match already..., {row_p =}, {row_c =}")
           elif filter_level3(row_p, row_c['cc_track'], row_c['cc_version'], row_c['cc_artist'], row_c['c_album'], row_c['c_platform'], song_versions): 
               if matched_level > 3:
                   row_p['refine_process_comment'] = 'Track, Artist Match'
                   row_p['refine_similarity'] = 3
                   best_match = {**row_p.to_dict(), **row_c.to_dict(), 'match_id': idx_p + 1}
                   matched_level = 3
               else: 
                   logging.debug(f"there is a better match already..., {row_p =}, {row_c =}")

       if matched_level < 4:
            match_results.append(best_match)
       else: 
           if filter_song_name(row_p, row_c['cc_track']):
               row_p['refine_process_comment'] = 'Track Match Only'
               row_p['refine_similarity'] = 4
               unmatch_results.append(row_p.to_dict())
           else: 
               logging.warning("No match found for platform track: {}".format(row_p['pc_track']))
               logging.debug(f"{row_p.to_dict() = }")
               logging.debug(f"{df_client}")
   logging.info("===== Matching completed.")
   
   matched_df = pd.DataFrame(match_results)
   unmatched_df = pd.DataFrame(unmatch_results)

   # get the unique song ids from the matched_df and unmatched_df
   matched_song_ids = tuple(matched_df['p_song_id'].unique().tolist()) if matched_df.shape[0] > 0 else []
   unmatched_song_ids = tuple(unmatched_df['p_song_id'].unique().tolist()) if unmatched_df.shape[0] > 0 else []

   # output the len of matched_id and unmatched_id and shape of the 2 dataframes. 
   logging.info(f"{len(matched_song_ids) = }, {len(unmatched_song_ids) = }, {df_client.shape[0] = }, {df_platform.shape[0] =}")

   # check if there's any common id in matched_id and unmatched_id, which should be empty
   common_song_ids = [x for x in matched_song_ids if x in unmatched_song_ids]
   if len(common_song_ids) ==0:
       logging.info("No common song ids found between matched and unmatched dfs.")
   else: 
       logging.warning("Common ids found. {}".format(', '.join(common_song_ids)))


   return matched_df, unmatched_df

##################################################################################################################

if __name__ == "__main__":
   run_matching_operation()

