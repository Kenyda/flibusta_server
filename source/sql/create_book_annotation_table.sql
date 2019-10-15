CREATE TABLE IF NOT EXISTS book_annotation
(
    book_id INTEGER NOT NULL PRIMARY KEY
        CONSTRAINT annotations_book_id_fkey REFERENCES book,
    title   VARCHAR(255) NOT NULL,
    body    text not null NOT NULL,
    file    VARCHAR(255)
);
ALTER TABLE book_annotation
    OWNER TO {};
CREATE INDEX IF NOT EXISTS book_annotations_content ON book_annotation (book_id);
