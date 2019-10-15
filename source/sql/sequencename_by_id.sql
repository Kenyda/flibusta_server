SELECT json_build_object(
           'id', ss.seq_id,
           'name', ss.name,
           'books', (
             SELECT array_to_json(array_agg(j.json_build_object))
             FROM (
                    SELECT json_build_object(
                               'id', book.id,
                               'title', book.title,
                               'lang', book.lang,
                               'file_type', book.file_type,
                               'annotation_exists', EXISTS(
                                   SELECT * FROM book_annotation WHERE book_annotation.book_id = book.id
                               ),
                               'authors', (
                                 SELECT array_to_json(array_agg(row_to_json(author)))
                                 FROM (SELECT id, first_name, last_name, middle_name FROM author) author
                                        LEFT JOIN bookauthor ba ON author.id = ba.author_id
                                 WHERE ba.book_id = book.id))
                    FROM book
                           LEFT JOIN seq ON book.id = seq.book_id
                    WHERE seq.seq_id = $2
                      AND lang = ANY ($1::text[])
                    ORDER BY seq.num
                    LIMIT $3 OFFSET $4) j))
FROM seqname ss
WHERE seq_id = $2