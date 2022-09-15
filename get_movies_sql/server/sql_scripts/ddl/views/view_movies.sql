use movies_db;
drop view if exists movies_view;
create view movies_view as
select movies_id,
       regexp_replace(movies_titles, %s, '') as movies_title,
       movies_genre,
       regexp_substr(movies_titles, %s)      as movies_year
from lnd_movies
where movies_titles regexp (%s)
  and movies_genre != (%s);