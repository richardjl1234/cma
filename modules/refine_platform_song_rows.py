import logging
import sys
import os
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    handlers=[
                              logging.StreamHandler()
                              ]
                    )
# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from settings import DataFeed, INCLUDE_SIMILARITY_LEVEL2

pd.options.mode.chained_assignment = None

class RefineLogicFuncNotDefined(Exception):
   pass 

############################################################################################
# refine logic for netease_max
def refine_logic_netease_max(data_feed: DataFeed, df_platform_song:pd.DataFrame ) -> pd.DataFrame:
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
    artist_names = artist_name.split(',')

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

    if INCLUDE_SIMILARITY_LEVEL2: 
        df_refined_level2 = df_platform_song.loc[df_platform_song.apply(filter_song_name, axis=1, args=(song_name, processed_albums, processed_artists,))]
        df_refined_level2['refine_process_comment'] = 'song_name exact match, but artist name and album name does not match'
        df_refined_level2['refine_similarity'] = 2
        logging.info("level 2 refined result is {}".format(df_refined_level2.shape))

        df_platform_song_refined = pd.concat([df_platform_song_refined_level1, df_refined_level2])

        return df_platform_song_refined
    else:
        logging.warning("only level 1 refined result is returned..., please update the settings.INCLUDE_SIMILARITY_LEVEL2 to be true if you need those level2 similarity rows to be returned.")
        return df_platform_song_refined_level1



########################################################################################################
# refine logic for kugou platform
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
    # the artist name should be a name which is separated by , 
    artist_names = artist_name.split(',')

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

    if INCLUDE_SIMILARITY_LEVEL2: 
        df_refined_level2 = df_platform_song.loc[df_platform_song.apply(filter_song_name, axis=1, args=(song_name, processed_albums, processed_artists,))]
        df_refined_level2['refine_process_comment'] = 'song_name exact match, but artist name and album name does not match'
        df_refined_level2['refine_similarity'] = 2
        logging.info("level 2 refined result is {}".format(df_refined_level2.shape))

        df_platform_song_refined = pd.concat([df_platform_song_refined_level1, df_refined_level2])
        return df_platform_song_refined

    else: 
        logging.warning("only level 1 refined result is returned..., please update the settings.INCLUDE_SIMILARITY_LEVEL2 to be true if you need those level2 similarity rows to be returned.")
        return df_platform_song_refined_level1

# define the refine logic for platform data
def refine_logics(platform):
    if platform == 'netease_max': 
        return refine_logic_netease_max
    elif platform == 'kugou':
        return refine_logic_kugou
    else: 
        logging.error(f'Invalid platform: {platform}, the refining logic has not been defined for this platform yet...')
        return None



def test_kugou():
    file_path = Path("/home/richard/shared/dropArea/upwork/CMA/phase1/output/debug")
    file_name = "Two Steps From Hell-kugou-el dorado.pkl"
    df = pd.read_pickle(file_path/file_name)
    df.to_csv('kugou1.csv', index=False)

    data_feed = (1, "Two Steps From Hell ", 'kugou','el dorado', ['burn', 'skyworld'])

    df_refined = refine_logic_kugou(data_feed, df)

    print(df_refined.shape)
    df_refined.to_csv('kugou2.csv', index=False)

def test_netease():
    file_path = Path("/home/richard/shared/dropArea/upwork/CMA/phase1/output/debug")
    file_name = "Two Steps From Hell-netease_max-el dorado.pkl"
    df = pd.read_pickle(file_path/file_name)
    df.to_csv('netease1.csv', index=False)

    data_feed = (1, "Two Steps From Hell,  thomas bergersen", 'netease_max','el dorado', ['burn', 'skyworld'])

    df_refined = refine_logic_netease_max(data_feed, df)

    print(df_refined.shape)
    df_refined.to_csv('netease2.csv', index=False)


if __name__ == '__main__':
    test_kugou()
    test_netease()