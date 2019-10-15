SELECT array_agg(seq.seq_id)
FROM seq
WHERE book_id = $1