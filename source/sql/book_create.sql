INSERT INTO book (id, title, lang, file_type, search_content)
VALUES ($1, cast($2 as varchar), $3, $4, to_tsvector($2))