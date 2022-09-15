use movies_db;
insert into movies (movie_id, movie_title, movie_genre, movie_year)
select *
from movies_view;