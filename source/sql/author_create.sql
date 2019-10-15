INSERT INTO author (id, first_name, middle_name, last_name, search_content)
VALUES ($1, $2, $3, $4, to_tsvector($5))