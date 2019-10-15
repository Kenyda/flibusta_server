UPDATE author
SET first_name     = $1,
    middle_name    = $2,
    last_name      = $3,
    search_content = to_tsvector($5)
WHERE id = $4