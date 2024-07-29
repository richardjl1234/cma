import pandas as pd


# open the two files into df1 and df2 and compare them
file1 = 'output/final_result.xlsx'
file2 = 'output/final_result_add_alias.xlsx'

df1 = pd.read_excel(file1, sheet_name='matched')
df2 = pd.read_excel(file2, sheet_name='matched')
df1 = df1.sort_values(by=['p_song_id'])
df2 = df2.sort_values(by=['p_song_id'])
# remove column match_id from the df1 and df2
df1 = df1.drop(columns=['match_id'])
df2 = df2.drop(columns=['match_id'])

for row1, row2 in zip(df1.iterrows(), df2.iterrows()):
    r1, r2 = row1[1], row2[1]
    idx1, idx2 = row1[0], row2[0]
    x = pd.DataFrame([r1, r2]).fillna('')
    x = x.T
    x.columns = ['df1', 'df2']
    
    x['diff'] = x.apply(lambda x: x['df1'] != x['df2'], axis=1)
    if any(list(x['diff'])):
        print(x)
    