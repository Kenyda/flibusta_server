SELECT COUNT(*)
FROM book
       LEFT JOIN bookauthor ba ON book.id = ba.book_id
WHERE ba.author_id = $1
  AND book.lang = ANY ($2::text[])