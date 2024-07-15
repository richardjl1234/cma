import pandas as pd
import re
import os


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
