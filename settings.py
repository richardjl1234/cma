from pathlib import Path
from modules.rds_access import ConnParams
import os

# TODO, the platform qqmusicv2 need to added, but now the performance is not good, so it is not ready to be added
PLATFORMS = ('netease_max', 'kugou')

LOG_PATH = Path('log')   # specify folder where the log files to be stored
OUTPUT_PATH = Path('output') # specify the folder where the output files to be stored

# ARTIST_NAME = "Two Steps From Hell"
# ARTIST_NAME = "Thomas Bergersen"
ARTIST_NAME = "Nick Phoenix"

# generate the connection parameter for a platform, it returns the connection parameters when connect to the postgresql database
def get_conn_params(platform):
    if platform not in PLATFORMS: 
        raise ValueError(f"invalid platform {platform}") 
        return None
    return ConnParams(os.environ["POSTGRE_HOST"], 5432,platform,  os.environ["POSTGRE_USER"], os.environ["POSTGRE_PWD"] ) 


# This is useful when we need to resume the process from the previous aborted process 
# TODO, the start and stop logic need to be implemented. 
SKIP_ARTIST = 0 # specfiy how many artist to be skipped
STOP_ARTIST = 99999 # specfiy when the process will be stopped

# TODO define the stop index for skip iteration and stop iterations for the first artist or the last artist 
SKIP_DATA_FEED = 0 # For the FIRST ARTIST to be processed, how many iterations to be skipped
STOP_DATA_FEED = 100 # specify how many iterations needed for the LAST ARTIST

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
