from pathlib import Path
from modules.rds_access import ConnParams
import os
import logging

LOG_LEVEL=logging.DEBUG

# TODO, the platform qqmusicv2 need to added, but now the performance is not good, so it is not ready to be added
PLATFORMS = ('netease_max', 'kugou')

LOG_PATH = Path('log')   # specify folder where the log files to be stored
OUTPUT_PATH = Path('output') # specify the folder where the output files to be stored

ARTIST_NAMES = [ "Two Steps From Hell", "Thomas Bergersen",  "Nick Phoenix"]

# generate the connection parameter for a platform, it returns the connection parameters when connect to the postgresql database
def get_conn_params(platform):
    if platform not in PLATFORMS: 
        raise ValueError(f"invalid platform {platform}") 
        return None
    return ConnParams(os.environ["POSTGRE_HOST"], 5432,platform,  os.environ["POSTGRE_USER"], os.environ["POSTGRE_PWD"] ) 


# This is useful when we need to resume the process from the previous aborted process 
START_ARTIST_INDEX = 0 # specfiy the start artist index. This index is INCLUDED
START_DATA_FEED_IDX = 0 # this index will be INCLUDED 

# default 99999 
END_ARTIST_INDEX = 0 # specfiy when the process will be stopped
END_DATA_FEED_IDX = 1 # this index will INCLUDED in the processing 


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
