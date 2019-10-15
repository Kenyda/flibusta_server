select count(*)
from seqname
where search_content @@ plainto_tsquery($1)
  and (
  select count(*)
  from seq
         inner join book b on seq.book_id = b.id and lang = ANY ($2::text[])
) <> 0