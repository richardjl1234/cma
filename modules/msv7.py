import pandas as pd
from fuzzywuzzy import fuzz
import logging
import sys, os

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from settings import   LOG_LEVEL
from modules.common import timeit

CLIENT_COLS = ['cc_track', 'cc_version', 'c_track', 'cc_artist', 'Unique Song ID', 'Unique Version ID', 'c_album', 'c_platform']
PLATFORM_COLS = ['p_song_id', 'p_track', 'pc_track', 'pc_artist', 'pc_version', 'refine_process_comment', 'refine_similarity', 'p_platform', 'p_album'] 


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
   client_df = client_df.groupby(['cc_track', 'cc_version', 'cc_artist', 'c_platform', 'c_album']).first().reset_index()
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

def save_to_excel_v2(matched_df, unmatched_df, output_path):
   """
   Save the results to the 'matched_results.xlsx' workbook.
   """
   logging.info("💾 Saving results to Excel...")
   with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
       # Save matched data to 'Matched' sheet
       matched_df.to_excel(writer, sheet_name='matched', index=False)
       # Save unmatched data to 'Matched' sheet starting from row after matched data
       unmatched_df.to_excel(writer, sheet_name='unmatched',  index=False)

   logging.info("📊 Data saved to Excel. Check the output file at: {}".format(str(output_path)) )

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


# check if the platform name match with each other
def filter_platform(row_p, c_platform):
    # return True
    a =   ('netease' in row_p['p_platform'].lower().strip() and 'netease' in c_platform.lower().strip())
    b =   ('kugou' in row_p['p_platform'].lower().strip() and 'kugou' in c_platform.lower().strip())
    # logging.info(f"THE PLATFORM CHECK.... {b = }, {row_p['p_platform'] =}, {c_platform =}"  )
    c =   ('qqmusic' in row_p['p_platform'].lower().strip() and 'qqmusic' in c_platform.lower().strip())
    d =   ('kuwo' in row_p['p_platform'].lower().strip() and 'kuwo' in c_platform.lower().strip())
    return a or b or c or d

def filter_song_name(row, song_name ):
    return row['pc_track'].lower().strip().strip('"').strip("'") == song_name.lower().strip().strip('"').strip("'")

# Define the filters below which need to be used in the refine process
# pc_artist could a list of artist which are separated by ,
def filter_artist_names(row, artist_names_c):
    artist_names_in_pc_artist_column = row['pc_artist'].split(',') 
    artist_names_in_cc_artist_column = artist_names_c.split(',')

    return any(name.lower().strip() == pc_artist_name.lower().strip()
               for name in artist_names_in_cc_artist_column 
               for pc_artist_name in artist_names_in_pc_artist_column) 

# define the filter that track name 
def filter_album_name(row, album_name):
    return album_name.lower().strip() == row['p_album'].lower().strip() 

# define the filter for song_versions
def filter_version(row, song_version):
    return song_version.lower().strip() == row['pc_version'].lower().strip() 

def filter_level1(row, song_name, song_version, artist_names, album_name, platform_name): 
    return filter_platform(row, platform_name) and filter_song_name(row, song_name) and filter_version(row, song_version) and filter_artist_names(row, artist_names) and filter_album_name(row, album_name) 

def filter_level2(row, song_name, song_version, artist_names, album_name, platform_name): 
    return filter_platform(row, platform_name) and filter_song_name(row, song_name) and filter_version(row, song_version) and filter_artist_names(row, artist_names) and (not filter_album_name(row, album_name)) 
        
def filter_level3(row, song_name, song_version, artist_names, album_name, platform_name): 
    return filter_platform(row, platform_name) and filter_song_name(row, song_name) and filter_artist_names(row, artist_names) 


@timeit
def match_tracks_v2(df_client, df_platform):

   logging.info("🔍 Matching tracks...")
   match_results = []
   unmatch_results = []

   for idx_p, row_p in df_platform.iterrows():
       for idx_c, row_c in df_client.iterrows():
           if filter_level1(row_p, row_c['cc_track'], row_c['cc_version'], row_c['cc_artist'], row_c['c_album'], row_c['c_platform']):
               row_p['refine_process_comment'] = 'Track, version, artist, album Exact Match'
               row_p['refine_similarity'] = 1
               match_results.append({**row_p.to_dict(), **row_c.to_dict(), 'match_id': idx_p + 1})
           elif filter_level2(row_p, row_c['cc_track'], row_c['cc_version'], row_c['cc_artist'], row_c['c_album'], row_c['c_platform']): 
               row_p['refine_process_comment'] = 'Track, Version, Artist Exact Match (album not match)'
               row_p['refine_similarity'] = 2
               match_results.append({**row_p.to_dict(), **row_c.to_dict(), 'match_id': idx_p + 1})
           elif filter_level3(row_p, row_c['cc_track'], row_c['cc_version'], row_c['cc_artist'], row_c['c_album'], row_c['c_platform']): 
               row_p['refine_process_comment'] = 'Track, Artist Match'
               row_p['refine_similarity'] = 3
               match_results.append({**row_p.to_dict(), **row_c.to_dict(), 'match_id': idx_p + 1})
           elif filter_song_name(row_p, row_c['cc_track']):
               row_p['refine_process_comment'] = 'Track Match Only'
               row_p['refine_similarity'] = 4
               unmatch_results.append(row_p.to_dict())
           else: 
               logging.warning("No match found for platform track: {}".format(row_p['pc_track']))
               logging.debug(f"{row_p.to_dict() = }")
               logging.debug(f"{row_c.to_dict() = }")
               continue
        
   logging.info("===== Matching completed.")

   return pd.DataFrame(match_results), pd.DataFrame(unmatch_results)


if __name__ == "__main__":
   run_matching_operation()

