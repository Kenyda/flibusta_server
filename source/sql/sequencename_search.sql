WITH filtered_seqnames AS (
       SELECT * FROM seqname, plainto_tsquery($1) f_query
       WHERE EXISTS (SELECT * FROM seq INNER JOIN book b ON seq.book_id = b.id AND lang = ANY($2::text[]))
              AND search_content @@ f_query
)
SELECT json_build_object(
       'count', (SELECT COUNT(*) FROM filtered_seqnames), 
       'result', (SELECT array_to_json(array_agg(j.json_build_object))
              FROM (SELECT json_build_object(
                 'id', seqname.seq_id,
                 'name', name,
                 'authors', (
                   SELECT array_to_json(array_agg(row_to_json(author)))
                   FROM (select a.id, a.first_name, a.last_name, a.middle_name
                         from seq
                                inner join book b on seq.book_id = b.id
                                inner join bookauthor b2 on b.id = b2.book_id
                                inner join author a on b2.author_id = a.id
                         where seq.seq_id = seqname.seq_id
                           and b.lang = any ($2::text[])
                         group by a.id
                         order by (select count(*)
                                   from seq
                                          inner join book b on seq.book_id = b.id
                                          inner join bookauthor b2 on b.id = b2.book_id and b2.author_id = a.id
                                   where seq.seq_id = seqname.seq_id) desc
                         ) author
                 ))
              FROM filtered_seqnames as seqname, plainto_tsquery($1) s_query
              ORDER BY ts_rank_cd(search_content, s_query) DESC,
                            LENGTH(name) DESC, name
              LIMIT $3 OFFSET $4) j
       )
) as json;