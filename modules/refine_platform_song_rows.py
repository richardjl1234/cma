import logging
import sys
import os
import pandas as pd
from pathlib import Path

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from settings import DataFeed, INCLUDE_REFINE_SIMILARITY_LEVEL, LOG_LEVEL

pd.options.mode.chained_assignment = None


# only return the rows that song_name match, while artist name and album name does not mathch
def filter_song_name(row, song_name ):
    return row['pc_track'].lower().strip() == song_name.lower().strip() 

# Define the filters below which need to be used in the refine process
def filter_artist_names(row, artist_names):
    return any(name.lower().strip() == row['pc_artist'].lower().strip() for name in artist_names) 

# define the filter that track name 
def filter_album_names(row, album_names):
    return any(name.lower().strip() == row['p_album'].lower().strip() for name in album_names) 

def filter_level1(row, song_name, artist_names): 
    return filter_song_name(row, song_name) and filter_artist_names(row, artist_names)

def filter_level2(row, song_name, artist_names, album_names): 
    return filter_song_name(row, song_name) and (not filter_artist_names(row, artist_names)) and  filter_album_names(row, album_names)
        
def filter_level3(row, song_name, artist_names, album_names): 
    return filter_song_name(row, song_name) and (not filter_artist_names(row, artist_names)) and (not filter_album_names(row, album_names)) 

############################################################################################
# refine logic for netease_max
def refine_platform_song_rows(data_feed: DataFeed, df_platform_song:pd.DataFrame ) -> pd.DataFrame:
    # unpack the data_feed input
    _, song_name, platform, artist_names, album_names = data_feed
    logging.info("the shape of the song '{}' dataframe (BEFORE Refined): {} ".format(song_name, df_platform_song.shape))
    
    df_platform_song['pc_track'] = df_platform_song['pc_track'].fillna('')
    df_platform_song['p_album'] = df_platform_song['p_album'].fillna('')
    df_platform_song['pc_artist'] = df_platform_song['pc_artist'].fillna('')

    df_level1 = df_platform_song.loc[df_platform_song.apply(filter_level1, axis=1, args=(song_name, artist_names,))]
    df_level1['refine_process_comment'] = 'Track Name exact match and Artist name exact match'
    df_level1['refine_similarity'] = 1

    # get all the rows which album name matches exactly
    df_level2 = df_platform_song.loc[df_platform_song.apply(filter_level2, axis=1, args=( song_name, artist_names, album_names,))]
    df_level2['refine_process_comment'] = 'Track Name exact match and ALBUM name exact match'
    df_level2['refine_similarity'] = 2

    df_level3 = df_platform_song.loc[df_platform_song.apply(filter_level3, axis=1, args=( song_name, artist_names, album_names,))]
    df_level3['refine_process_comment'] = 'Track Name exact match only'
    df_level3['refine_similarity'] = 3

    df_platform_song_refined = pd.concat([df_level1, df_level2, df_level3])

    # now get the counts for each simlarity level from the df_platform_song_refined 
    counts = df_platform_song_refined.groupby('refine_similarity').size()
    for level, count in dict(counts).items():
        logging.info(" There are {} records in level {}...".format(count, level))

    logging.info(f"The value of {INCLUDE_REFINE_SIMILARITY_LEVEL = }...")
    df_refine_final =  df_platform_song_refined.loc[df_platform_song_refined['refine_similarity'] <= INCLUDE_REFINE_SIMILARITY_LEVEL]
    logging.info("the shape of the song '{}' dataframe (AFTER refined): {} ".format(song_name, df_refine_final.shape))
    return df_refine_final

# TODO this test method need to be rewrite
def test_refine_logic():
    file_path = Path("/home/richard/shared/dropArea/upwork/CMA/phase1/output/debug")
    file_name = "netease_max-mohicans.pkl"
    df = pd.read_pickle(file_path/file_name)
    df.to_csv('netease1.csv', index=False)
    data_feed = (1, "mohicans", 'netease_max',('anthony keyrouz', ), ('mohicans', ))

    df_refined = refine_platform_song_rows(data_feed, df)

    print(df_refined.shape)
    df_refined.to_csv('netease2.csv', index=False)

if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL,
                        format='%(asctime)s : %(levelname)s : %(message)s',
                        handlers=[ logging.StreamHandler() ])
    test_refine_logic()