# TODO need to check if the leading % is needed or not

SQL_SONG = {"qqmusicv2": """
SELECT song_mid, song_name, album_mid, album_name, artist_mid, singer_names,
comment_number, singer_mids, company, release_date
FROM songs
where song_name ilike '{song_name}%'
""", 
#  WHERE '002Vj8o12l10eK' = ANY(singer_mids)
#  or '001fbZH44WRuKW' = ANY(singer_mids)
#  or '000Eqw823oivl5' = ANY(singer_mids)

"netease_max": """
SELECT album_id, song_id, song_name, artist_id, copyright_id, deprecated_artist_name, artist_ids,
comment_count, audit_songs.artist_array, album_name, release_date, company, song_array
FROM audit_songs
INNER JOIN audit_albums USING (album_id)
where song_name ilike '{song_name}%'
""", 
# where '102714' = any(audit_songs.artist_array)
# or '792429' = any(audit_songs.artist_array)
# or '45336' = any(audit_songs.artist_array)
"kugou":"""
SELECT ks.*, ka.album_name
FROM kugou_songs ks
LEFT JOIN kugou_albums ka ON ks.album_id = ka.album_id
where work_name ilike '{song_name}%'
"""}
# WHERE 153413 = ANY(ks.singer_ids)
#   OR 175750 = ANY(ks.singer_ids)
#   OR 153666 = ANY(ks.singer_ids);

