from pathlib import Path
from modules.rds_access import ConnParams
import os
import logging
from collections import namedtuple

# LOG_LEVEL=logging.INFO
LOG_LEVEL=logging.INFO

# output format for summary and detail

# artist alias mapping
ARTIST_ALIAS = {
    "Two Steps From Hell": "Two Steps From Hell, Thomas Bergerson, Thomas Bergersen"
}

# The platform names from client statements (in lower case)
PLATFORM_IN_SCOPE_CLIENT_STATEMENT = ['netease', 'tencent']

# The platform database names to be included
PLATFORMS_DB_IN_SCOPE = ('netease_max', 'kugou', 'qqmusicv2')
# PLATFORMS = ('qqmusicv2', )
# PLATFORMS = ( 'kugou', )

LOG_PATH = Path('log')   # specify folder where the log files to be stored
OUTPUT_PATH = Path('output') # specify the folder where the output files to be stored

# the input file should be in the input_data folder, the format can be xlsx, csv or pkl
INPUT_PATH = "input_data"
# INPUT_FILE = "cc_soave.pkl"  
# INPUT_FILE = "Two Steps Test.xlsx"  
# INPUT_FILE = "cc_soave.xlsx"  
INPUT_FILE = "cc_twosteps.xlsx"  

# This is useful when we need to resume the process from the previous aborted process 
START_SONG_INDEX = 0  # specfiy the start song index. This index is INCLUDED
END_SONG_INDEX = 50 # specfiy when the process will be stopped

# include the refine_similarity_level_2 records, level 2 are the rows with exact matching on song name, but does not match on singer name and alblum name
# similarity level 1, song name match + artist name match + album name exact match
# similarity level 2, song name match + album name match
# similarity level 3, song name match only
# when the value is 1, it will only return the level 1 rows
# when the value is 2, it will return the level 1 and 2 rows
# when the value is 3, it will return all the rows
# default, we should use 2, which means we will return the level 1 and 2 rows
# INCLUDE_REFINE_SIMILARITY_LEVEL = 3 

##############################################################
# the column mapping from query result to the pc_columns
COLUMN_MAPPING = {
    'qqmusicv2': {
        'song_mid': 'p_song_id',
        'song_name': 'p_track',
        'album_mid': 'p_album_id',
        'album_name': 'p_album',
        'singer_mids': 'p_artist_id',
        'singer_names': 'p_artist',
        'company': 'p_company',
        'release_date': 'p_release_date',
        'comment_number': 'p_comments'
    },
    'netease_max': {
        'song_id': 'p_song_id',
        'song_name': 'p_track',
        'album_id': 'p_album_id',
        'album_name': 'p_album',
        'artist_ids': 'p_artist_id',
        'deprecated_artist_name': 'p_artist',
        'company': 'p_company',
        'release_date': 'p_release_date',
        'comment_count': 'p_comments'}, 
    'kugou': {
        'audio_id': 'p_song_id',
        'work_name': 'p_track',
        'album_id': 'p_album_id',
        'album_name': 'p_album',
        'singer_ids': 'p_artist_id',
        'ori_author_name': 'p_artist',
        'publish_company': 'p_company',
        'publish_date': 'p_release_date',
        'combine_count': 'p_comments'
    }

}

##############################################################
# generate the connection parameter for a platform, it returns the connection parameters when connect to the postgresql database
def get_conn_params(platform):
    if platform not in PLATFORMS_DB_IN_SCOPE: 
        raise ValueError(f"invalid platform {platform}") 
        return None
    return ConnParams(os.environ["POSTGRE_HOST"], 5432,platform,  os.environ["POSTGRE_USER"], os.environ["POSTGRE_PWD"] ) 

##############################################################
DataFeed = namedtuple("DataFeed", ['song_index', 'song_name', 'platform', 'artist_names', 'album_names', 'song_versions'])