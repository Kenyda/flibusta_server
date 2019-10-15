SELECT json_build_object(
    'author_id', author_annotation.author_id,
    'title', author_annotation.title,
    'body', author_annotation.body,
    'file', author_annotation.file
)
FROM author_annotation
WHERE author_annotation.author_id = $1
