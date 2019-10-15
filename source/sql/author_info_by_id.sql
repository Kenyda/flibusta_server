SELECT json_build_object(
           'id', author.id,
           'first_name', author.first_name,
           'last_name', author.last_name,
           'middle_name', author.middle_name,
           'annotation_exists', EXISTS(
                SELECT * FROM author_annotation WHERE author_annotation.author_id = author.id
            )
           )
FROM author
WHERE author.id = $1;