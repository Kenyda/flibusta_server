CREATE TABLE IF NOT EXISTS author_annotation
(
    author_id INTEGER NOT NULL PRIMARY KEY
        CONSTRAINT annotations_author_id_fkey REFERENCES author,
    title   VARCHAR(255) NOT NULL,
    body    text not null NOT NULL,
    file    VARCHAR(255)
);
ALTER TABLE author_annotation OWNER TO {};
CREATE INDEX IF NOT EXISTS author_annotations_content ON author_annotation (author_id);
