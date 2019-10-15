SELECT COUNT(*)
FROM author
WHERE author.search_content @@ plainto_tsquery($1)
  AND (SELECT count(*)
       FROM bookauthor
              RIGHT JOIN book ON bookauthor.book_id = book.id
       WHERE book.lang = ANY ($2::text[])
         AND bookauthor.author_id = author.id) <> 0