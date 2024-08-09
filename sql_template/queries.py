# TODO need to check if the leading % is needed or not

SQL_SONG = {"qqmusicv2": """
SELECT song_mid, song_name, album_mid, album_name, artist_mid, singer_names,
comment_number, singer_mids, company, release_date, 
'NA' as p_likes_count, 'NA' as p_stream_count_1, 'NA' as p_stream_count_2
FROM songs
where song_name ilike '%{song_name}%'
""", 
#  WHERE '002Vj8o12l10eK' = ANY(singer_mids)
#  or '001fbZH44WRuKW' = ANY(singer_mids)
#  or '000Eqw823oivl5' = ANY(singer_mids)

"netease_max": """
SELECT audit_songs.album_id, audit_songs.song_id, audit_songs.song_name, audit_songs.artist_id, 
copyright_id, deprecated_artist_name, artist_ids,
audit_songs.comment_count, audit_songs.artist_array, album_name, release_date, 
company, song_array, likes as p_likes_count, 
award.play_num as p_stream_count_1, 
-- award.comment_count as award_comment_count, 
top_songs.play_count as p_stream_count_2
FROM audit_songs
INNER JOIN audit_albums on audit_songs.album_id = audit_albums.album_id
left outer JOIN honor_single_award as award on award.song_id = audit_songs.song_id
left outer JOIN honor_top_songs as top_songs on audit_songs.song_id::text = top_songs.song_id
where audit_songs.song_name ilike '%{song_name}%'
""", 
# where '102714' = any(audit_songs.artist_array)
# or '792429' = any(audit_songs.artist_array)
# or '45336' = any(audit_songs.artist_array)
"kugou":"""
SELECT audio_id, singer_id, ks.language, ori_author_name, 
work_name, ks.cid, ks.scrape_date, ks.combine_count, 
'NA' as p_likes_count, 'NA' as p_stream_count_1, 'NA' as p_stream_count_2,
ks.album_id, ks.publish_company, 
ks.raw_json_1, singer_ids, singer_names, combine_count_v2, 
ka.album_name
FROM kugou_songs ks
LEFT JOIN kugou_albums ka ON ks.album_id = ka.album_id
where work_name ilike '%{song_name}%'
"""}
# WHERE 153413 = ANY(ks.singer_ids)
#   OR 175750 = ANY(ks.singer_ids)
#   OR 153666 = ANY(ks.singer_ids);

