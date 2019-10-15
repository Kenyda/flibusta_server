SELECT COUNT(*)
FROM book
WHERE id IN (SELECT book_id FROM seq WHERE seq_id = $1)
  AND book.lang = ANY ($2::text[])