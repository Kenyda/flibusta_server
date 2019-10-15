SELECT COUNT(*)
FROM book
WHERE lang = ANY ($1::text[])
  AND book.search_content @@ plainto_tsquery($2)