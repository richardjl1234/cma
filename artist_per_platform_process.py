from  pathlib import Path
import logging
import pandas as pd



def artist_per_platform_process(df_artist_filtered, platform):
    """
    This program is for ONE ARTIST on ONE platform, It recevies 2 arguments
    this program is the main program which integrate all the matching process for the music audit for a SINGLE ARTIST
    step 1: read the client statement from excel/csv file, for a given artist name in the sheet, extract all song names, versions and alblum name
    step 2: based on the song names, versions and album names, perform the fuzzy matching to identify the unique id of the song, album and artist
    step 3: 

    input: df_artist, platform, df_artist_filtered is the cleansed dataframe which only have one artist name, (with cc_ columns)
    output: 
    """

    # step 1. read the client statement from excel/csv file, for a given artist name in the sheet, extract all song names, versions and alblum name

    pass







if __name__ == '__main__':
    # read the excel file to dataframe
    logging.info("read the execl file to memory...")
    input_path = Path('input_data')
    excel_file_path = input_path / 'cc_twosteps.xlsx'
    df_artist = pd.read_excel(excel_file_path)

    artist_name = 'Two Steps From Hell'
    logging.info('start to process the artist: {}'.artist_name)

    # Get the data for the artist name only from the dataframe df_artist
    artists = df_artist['c_artist'].unique()
    logging.debug("there are {} in the dataframe, the list of the artist name are: ".format(len(artists), ', '.join(artists)))
    df_artist_filtered = df_artist.loc[df_artist['c_artist'] == artist_name]




    # artist_per_platform_process(, 'kugou')    