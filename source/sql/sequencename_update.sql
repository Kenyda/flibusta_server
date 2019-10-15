UPDATE seqname
SET name           = cast($2 as varchar),
    search_content = to_tsvector($2)
WHERE seq_id = $1