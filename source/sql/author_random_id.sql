SELECT author.id
       FROM author
       WHERE (SELECT count(*)
              FROM bookauthor
                     RIGHT JOIN book ON bookauthor.book_id = book.id
              WHERE book.lang = ANY ($1::text[])
                AND bookauthor.author_id = author.id) <> 0
       ORDER BY random()
       LIMIT 1;