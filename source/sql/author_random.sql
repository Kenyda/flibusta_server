select author.id
       from author
       where (select count(*)
              from bookauthor
                     right join book on bookauthor.book_id = book.id
              where book.lang = ANY ($1::text[])
                and bookauthor.author_id = author.id) <> 0
       order by random()
       LIMIT 1;
