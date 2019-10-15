SELECT json_build_object(
    'book_id', book_annotation.book_id,
    'title', book_annotation.title,
    'body', book_annotation.body,
    'file', book_annotation.file
)
FROM book_annotation
WHERE book_annotation.book_id = $1