WITH filtered_authors AS (
  SELECT *
  FROM author, plainto_tsquery($1) f_query
  WHERE EXISTS (SELECT *
    FROM bookauthor
            RIGHT JOIN book ON bookauthor.book_id = book.id
    WHERE book.lang = ANY ($2::text[])
      AND bookauthor.author_id = author.id) AND author.search_content @@ f_query
)
SELECT json_build_object(
  'count', (SELECT COUNT(*) FROM filtered_authors),
  'result', (SELECT array_to_json(array_agg(j.json_build_object))
    FROM (
        select json_build_object(
                    'id', author.id,
                    'first_name', author.first_name,
                    'last_name', author.last_name,
                    'middle_name', author.middle_name,
                    'annotation_exists', EXISTS(
                      SELECT * FROM author_annotation WHERE author_annotation.author_id = author.id
                    )
        )
        from filtered_authors as author, plainto_tsquery($1) s_query
        order by ts_rank(author.search_content, s_query) DESC,
                  (select count(*)
                  from bookauthor
                  where bookauthor.author_id = author.id) DESC, author.id
        LIMIT $3 OFFSET $4) j
  )
) as json
