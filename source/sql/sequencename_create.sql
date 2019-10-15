INSERT INTO seqname (seq_id, name, search_content)
VALUES ($1, cast($2 as varchar), to_tsvector($2))