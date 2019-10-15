SELECT json_build_object(
           'id', author.id,
           'first_name', author.first_name,
           'last_name', author.last_name,
           'middle_name', author.middle_name,
           'annotation_exists', EXISTS(
              SELECT * FROM author_annotation WHERE author_annotation.author_id = author.id
            ),
           'books', (
             SELECT array_to_json(array_agg(row_to_json(book)))
             FROM (
                    SELECT id, title, lang, file_type,
                    EXISTS(SELECT * FROM book_annotation WHERE book_annotation.book_id = book.id) as annotation_exists
                    FROM book
                    WHERE id IN (
                      SELECT b.id
                      FROM book b
                             LEFT JOIN bookauthor b2 on b.id = b2.book_id
                      WHERE b.lang = ANY ($1::text[])
                        AND b2.author_id = author.id)
                    ORDER BY title
                    LIMIT $3 OFFSET $4) book))
FROM author
WHERE author.id = $2