DELETE
FROM bookauthor
WHERE book_id = $1
  AND author_id = $2