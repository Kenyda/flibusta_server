CREATE TABLE IF NOT EXISTS bookauthor
(
  id        serial  NOT NULL,
  book_id   INTEGER NOT NULL
    CONSTRAINT bookauthor_book_id_fkey REFERENCES book,
  author_id INTEGER NOT NULL
    CONSTRAINT bookauthor_author_id_fkey REFERENCES author,
  PRIMARY KEY (book_id, author_id)
);
ALTER TABLE bookauthor
  OWNER TO {};
CREATE INDEX IF NOT EXISTS bookauthor_book_id ON bookauthor (book_id);
CREATE INDEX IF NOT EXISTS bookauthor_author_id ON bookauthor (author_id);