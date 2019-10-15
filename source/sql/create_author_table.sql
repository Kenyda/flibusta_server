CREATE TABLE IF NOT EXISTS author
(
  id             INTEGER      NOT NULL
    CONSTRAINT author_pkey PRIMARY KEY,
  first_name     VARCHAR(128) NOT NULL,
  last_name      VARCHAR(128) NOT NULL,
  middle_name    VARCHAR(128) NOT NULL,
  search_content tsvector     NOT NULL
);
ALTER TABLE author
  OWNER TO {};
CREATE INDEX IF NOT EXISTS author_search_content ON author (search_content);