use movies_db;
drop view if exists result_movies_view;
create view result_movies_view as
select movies_id, movies_title, movies_genre, movies_year, r.movie_rating as movies_rating
from movies_view m
         join rating_view r on m.movies_id = r.movie_id;