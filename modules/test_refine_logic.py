from pc_platform import refine_logic_kugou
import os 
from pathlib import Path

# read the csv file into dataframe
import pandas as pd
import logging

# turn of pandas warning SettingWithCopyWarning
pd.options.mode.chained_assignment = None 

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s : %(levelname)s : %(message)s',
                    handlers=[
                              logging.StreamHandler()
                              ]
                    )


file_path = Path("/home/richard/shared/dropArea/upwork/CMA/phase1/output/debug")
file_name = "Two Steps From Hell-kugou-el dorado.pkl"
df = pd.read_pickle(file_path/file_name)
df.to_csv('temp1.csv', index=False)

albums = ['burn', 'skyworld']
artist_names  =  ["Two Steps From Hell "]
additional_artists = []
additional_albums = []

# columns = list(filter(lambda x: x.startswith('p'), df.columns))
# print(columns)

# ft_exact_track_name = df['pc_track'].str.lower().str.strip() == 'el dorado'
# df1 = df.loc[ft_exact_track_name, columns]

# ft_exact_album_name = df['p_album'].str.lower().str.strip().isin(albums)
# ft_exact_album_name = df['p_album'].str.lower().str.strip().isin(map(lambda x: x.lower().strip(),    additional_albums))
# df2 = df.loc[ft_exact_album_name, columns]
# additional_artists = df2['pc_artist'].unique()
# print(additional_artists)


# ft_exact_singer_name = df['pc_artist'].str.lower().str.strip().isin(map(lambda s: s.lower().strip(), artist_names)) 
# df3 = df.loc[ft_exact_singer_name, columns]
# additional_albums = df3['p_album'].dropna().unique()
# print(additional_albums)


data_feed = (1, "Two Steps From Hell ", 'kugou','el dorado', ['burn', 'skyworld'])

df_refined = refine_logic_kugou(data_feed, df)

print(df_refined.shape)
df_refined.to_csv('temp2.csv', index=False)
# df_refined.to_excel('temp.xlsx')