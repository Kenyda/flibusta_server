SELECT json_build_object(
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
                         limit 6) author
                 ))
      FROM seqname
      WHERE search_content @@ plainto_tsquery($1) AND (SELECT count(*) FROM seq inner join book b
          ON seq.seq_id = seqname.seq_id) <> 0
      ORDER BY random()
      LIMIT 1;
