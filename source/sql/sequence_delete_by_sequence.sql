DELETE
FROM seq
WHERE seq_id = ANY ($1::int[])