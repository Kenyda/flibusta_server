UPDATE book
SET title          = cast($1 as varchar),
    lang           = cast($2 as varchar),
    file_type      = cast($3 as varchar),
    search_content = to_tsvector($1)
WHERE id = $4