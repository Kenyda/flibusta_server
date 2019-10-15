DELETE
FROM seq
WHERE book_id = $1
  AND seq_id = $2