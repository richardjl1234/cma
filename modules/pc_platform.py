import pandas as pd
import re
import os
import logging
import numpy as np
import sys

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from settings import DataFeed

pd.options.mode.chained_assignment = None

class RefineLogicFuncNotDefined(Exception):
   pass 

# Define the regex patterns for extraction and removal
EXTRACTION_PATTERNS = [
   (r"[\( \[](?:Ft|Feat|W\/|\.| )([^()\[\]]+)[\) \]]", "additional_artist"),
   (r"- (Instrumental)", "version_instrumental"),
   (r"[\( \[]([^()]+)[\) \]]$", "version_general"),
   (r"- (EP)", "version_ep"),
   (r"[\#\~\+\-]\s*(.*)", "version_misc"),
]


def clean_artist_name(artist_name):
   if pd.isna(artist_name):
       return artist_name  # Handle NaN values safely
   return artist_name.replace("{", "").replace("}", "").replace("\"", "")


def extract_attributes(title, pattern):
   if pd.isna(title):
       return ''
   match_results = []
   while True:
       match = re.search(pattern, title.strip(), re.IGNORECASE)
       if match:
           match_results.append(match.group(1).strip().replace(".", ""))
           title = re.sub(pattern, "", title.strip(), flags=re.IGNORECASE)
       else:
           break
   return ",".join(match_results)


def remove_substrings(title, pattern):
   if pd.isna(title):
       return ''
   while True:
       match = re.search(pattern, title.strip(), re.IGNORECASE)
       if match:
           title = re.sub(pattern, "", title.strip(), flags=re.IGNORECASE)
       else:
           break
   return title.replace(".", "")


def remove_duplicate_values(names):
   if pd.isna(names):
       return ''
   unique_values = ",".join(sorted(set([name.strip() for name in names.split(",")])))
   return unique_values


def clean_song_data(raw_data):
   raw_data["pc_track"] = raw_data["p_track"].apply(lambda x: x.replace("（", " (").replace("）", ") ") if pd.notna(x) else x)


   for pattern, attr in EXTRACTION_PATTERNS:
       raw_data[attr] = raw_data["pc_track"].apply(extract_attributes, args=(pattern,))
       raw_data["pc_track"] = raw_data["pc_track"].apply(remove_substrings, args=(pattern,))


   raw_data["pc_artist"] = raw_data["p_artist"].apply(clean_artist_name)


   raw_data["pc_artist"] = raw_data.apply(
       lambda row: ", ".join(filter(None, [row["pc_artist"], row.get("additional_artist", "")])).strip(), axis=1
   )


   raw_data["pc_artist"] = raw_data["pc_artist"].apply(remove_duplicate_values)


   raw_data["pc_version"] = raw_data.apply(
       lambda row: ", ".join(filter(None, [row[col] for col in raw_data.columns if "version" in col])).strip(),
       axis=1,
   )


   # Standardizing 'pc_version' and 'pc_track' as per new requirements
   raw_data['pc_version'] = raw_data['pc_version'].fillna('generic').str.lower().str.strip()
   raw_data['pc_track'] = raw_data['pc_track'].str.lower().str.strip()


   # Return the entire DataFrame with all original and new columns
   return raw_data


def merge_data(qq_path, netease_path, kugou_path, output_path):
   if not all(os.path.exists(path) for path in [qq_path, netease_path, kugou_path]):
       print("Error: One or more input files not found.")
       return


   df_qq = pd.read_csv(qq_path)
   df_netease = pd.read_csv(netease_path)
   df_kugou = pd.read_csv(kugou_path)


   df_qq = df_qq.rename(columns={
       'song_mid': 'p_song_id',
       'song_name': 'p_track',
       'album_mid': 'p_album_id',
       'album_name': 'p_album',
       'singer_mids': 'p_artist_id',
       'singer_names': 'p_artist',
       'company': 'p_company',
       'release_date': 'p_release_date',
       'comment_number': 'p_comments'
   }).assign(p_platform='QQMusic')


   df_netease = df_netease.rename(columns={
       'song_id': 'p_song_id',
       'song_name': 'p_track',
       'album_id': 'p_album_id',
       'album_name': 'p_album',
       'artist_ids': 'p_artist_id',
       'deprecated_artist_name': 'p_artist',
       'company': 'p_company',
       'release_date': 'p_release_date',
       'comment_count': 'p_comments'
   }).assign(p_platform='NetEase')


   df_kugou = df_kugou.rename(columns={
       'audio_id': 'p_song_id',
       'work_name': 'p_track',
       'album_id': 'p_album_id',
       'album_name': 'p_album',
       'singer_ids': 'p_artist_id',
       'ori_author_name': 'p_artist',
       'publish_company': 'p_company',
       'publish_date': 'p_release_date',
       'combine_count': 'p_comments'
   }).assign(p_platform='Kugou')


   master_df = pd.concat([df_qq, df_netease, df_kugou], ignore_index=True)
   master_df.to_csv(output_path, index=False)
   print(f'Merged data saved to {output_path}')


   return master_df


# TODO refine logic for df_platform_song_raw
def refine_logic_netease_max(data_feed: DataFeed, df_platform_song):
    return df_platform_song

# TODO refine logic for df_platform_song_raw
def refine_logic_kugou(data_feed: DataFeed, df_platform_song:pd.DataFrame ) -> pd.DataFrame:
    logging.info("the shape of the input dataframe is {}".format(df_platform_song.shape))
    
    df_platform_song['pc_track'] = df_platform_song['pc_track'].fillna('')
    df_platform_song['p_album'] = df_platform_song['p_album'].fillna('')
    df_platform_song['pc_artist'] = df_platform_song['pc_artist'].fillna('')

    # define the filters

    def filter_artist_names(row, artist_names):
        return any(name.lower().strip() == row['pc_artist'].lower().strip() for name in artist_names)

    def filter_album_names(row, album_names):
        return any(name.lower().strip() == row['p_album'].lower().strip() for name in album_names)

    def add_refine_comment(row, album_names, artist_names):
        text = ''
        if filter_album_names(row, album_names):
            text += 'Album name exact match, '
        if filter_artist_names(row, artist_names):
            text += 'Artist name exact match, '

        return text

    # only return the rows that song_name match, while artist name and album name does not mathch
    def filter_song_name(row, song_name, album_names, artist_names):
        if filter_album_names(row, album_names) or filter_artist_names(row, artist_names):
            return False
        if row['pc_track'].lower().strip() == song_name.lower().strip(): 
            return True
        else: 
            return False
            

    # unpack the data_feed input
    _, artist_name, _, song_name, album_names = data_feed
    artist_names = [artist_name]

    processed_artists, processed_albums = set(), set()  # the set which have been processed
    artist_names = set(map(lambda x: x.lower().strip(), artist_names)) # the names of artist to be process in the next iteration 
    album_names = set(map(lambda x: x.lower().strip(), album_names)) # the name of the albums to be processed in the next iteration

    logging.info(f"Start iteratively filtering by {song_name},  {artist_names}, {album_names}")
    df_platform_song_refined_level1 = pd.DataFrame()

    # print the information of processed artists and albums
    logging.info(f"Processed artists: {processed_artists}, processed albums: {processed_albums}")
    # print the information of artist_names and album_names
    logging.info(f"Artists to be processed: {artist_names.difference(processed_artists)}, remaining albums: {album_names.difference(processed_albums)}")  

    iteration_cnt = 0
    print('--------------------')
    print(artist_names)
    print(processed_artists)
    print(album_names)
    print(processed_albums)
    print('--------------------')
    print(artist_names - processed_artists)
    print(album_names - processed_albums)
    print('--------------------')

    while(len(artist_names - processed_artists) != 0  or len(album_names - processed_albums) != 0) :
        if iteration_cnt > 21: 
            logging.warning("The iteration count exceeds the threshold 21, break the loop...") 
            break; 

        iteration_cnt += 1  
        logging.info(f"Iteration {iteration_cnt}:")

        # get all the rows which artist name matches exactly

        df2 = df_platform_song.loc[df_platform_song.apply(filter_artist_names, axis=1, args=(artist_names, ))]
        # df2['refine_process_comment'] = df2['refine_process_comment'].apply(lambda x: list(set(x.append('Artist Name exact match'))))
        df_platform_song_refined_level1 = pd.concat([df_platform_song_refined_level1, df2])

        # get all the rows which album name matches exactly
        df3 = df_platform_song.loc[df_platform_song.apply(filter_album_names, axis=1, args=(album_names,))]
        # df3['refine_process_comment'] = df3['refine_process_comment'].apply(lambda x: list(set(x.append( 'Album name exact match'))))
        df_platform_song_refined_level1 = pd.concat([df_platform_song_refined_level1, df3])
        # df_platform_song_refined.drop_duplicates(inplace=True)
        df_platform_song_refined_level1 = df_platform_song_refined_level1.groupby('p_song_id').first().reset_index(names = ['p_song_id'])

        # df_platform_song_refined = df_platform_song_refined.loc[df_platform_song_refined.astype(str).drop_duplicates().index]

        # copy the artist_names, album_names to the processed_artists, precessed_albums to avoid duplicate processing
        processed_artists |= artist_names
        processed_albums |= album_names

        # identify to be processed artist_names and album names which need to processed in the next iteration 
        artist_names = set(filter(lambda x: x!='', df_platform_song_refined_level1['pc_artist'].dropna().str.lower().str.strip().unique().tolist()))
        album_names = set(filter(lambda x: x!='',df_platform_song_refined_level1['p_album'].dropna().str.lower().str.strip().unique().tolist()))

        # print the the information of artist_names and album_names
        logging.info(f"Identified artists: {artist_names}, identified albums: {album_names}, they will be processed in next iteration if they have not all processed yet...")

        # print the information of processed artists and albums
        logging.info(f"Processed artists: {processed_artists}, processed albums: {processed_albums}")
        

    # sort the df_platform_song_refined by refine_similarity and refine_process_comment in descending order
    # df_platform_song_refined_level1.sort_values(by=['refine_similarity'], ascending=[True ], inplace=True)
    df_platform_song_refined_level1['refine_process_comment'] = df_platform_song_refined_level1.apply(add_refine_comment, axis=1, args=(album_names, artist_names, ))
    df_platform_song_refined_level1['refine_similarity'] = 1
    logging.info("level 1 refined result is {}".format(df_platform_song_refined_level1.shape))

    print(processed_albums, processed_artists)
    df_refined_level2 = df_platform_song.loc[df_platform_song.apply(filter_song_name, axis=1, args=(song_name, processed_albums, processed_artists,))]
    df_refined_level2['refine_process_comment'] = 'song_name exact match, but artist name and album name does not match'
    df_refined_level2['refine_similarity'] = 2
    logging.info("level 2 refined result is {}".format(df_refined_level2.shape))

    df_platform_song_refined = pd.concat([df_platform_song_refined_level1, df_refined_level2])
    

    return df_platform_song_refined

# define the refine logic for platform data
def refine_logics(platform):
    if platform == 'netease_max': 
        return refine_logic_netease_max
    elif platform == 'kugou':
        return refine_logic_kugou
    else: 
        logging.error(f'Invalid platform: {platform}, the refining logic has not been defined for this platform yet...')
        return None


def main():
   base_path = '/Users/CallumAmor/Dropbox/My Mac (Cal.fios-router.home)/Downloads'
   qq_path = os.path.join(base_path, 'ni/co qq.csv')
   netease_path = os.path.join(base_path, 'ni/co netease.csv')
   kugou_path = os.path.join(base_path, 'ni/co kugou.csv')
   merged_output_path = os.path.join(base_path, 'merged_platform_data_ni/co.csv')


   merged_df = merge_data(qq_path, netease_path, kugou_path, merged_output_path)
   cleaned_df = clean_song_data(merged_df)


   cleaned_output_path = os.path.join(base_path, 'pc_twosteps.csv')
   cleaned_df.to_csv(cleaned_output_path, index=False)
   print(f'Cleaned data saved to {cleaned_output_path}')


   # Create Excel sheet with p_song_id from NetEase and QQMusic
   song_ids = cleaned_df.loc[(cleaned_df['p_platform'] == 'NetEase') | (cleaned_df['p_platform'] == 'QQMusic'), ['p_song_id', 'p_comments', 'p_platform']]
   song_ids_output_path = os.path.join(base_path, 'song_idsV20.xlsx')
   song_ids.to_excel(song_ids_output_path, index=False)
   print(f'Song IDs Excel sheet saved to {song_ids_output_path}')


if __name__ == "__main__":
   main()
