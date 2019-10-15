SELECT array_to_json(array_agg(j.json_build_object))
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
       from author
       where author.search_content @@ plainto_tsquery($1)
         and (select count(*)
              from bookauthor
                     right join book on bookauthor.book_id = book.id
              where book.lang = ANY ($2::text[])
                and bookauthor.author_id = author.id) <> 0
       order by ts_rank(author.search_content, plainto_tsquery($1)) DESC,
                (select count(*)
                 from bookauthor
                 where bookauthor.author_id = author.id) DESC, author.id
       LIMIT $3 OFFSET $4) j