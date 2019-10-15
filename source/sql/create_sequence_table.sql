CREATE TABLE IF NOT EXISTS seq
(
  book_id INTEGER NOT NULL
    CONSTRAINT seq_book_id_fkey REFERENCES book,
  seq_id  INTEGER NOT NULL
    CONSTRAINT seq_seq_id_fkey REFERENCES seqname,
  num     INTEGER NOT NULL,
  PRIMARY KEY (book_id, seq_id)
);
ALTER TABLE seq
  OWNER TO {};
CREATE INDEX IF NOT EXISTS sequence_book_id ON seq (book_id);
