use movies_db;
drop view if exists movies_and_rating_seperated_genres;
create view movies_and_rating_seperated_genres as
(
with recursive movies_with_parsed_genres as (
    select movies_id,
           movies_title,
           substring_index(movies_genre, '|', 1)                                           as genre,
           substring(movies_genre, char_length(substring_index(movies_genre, '|', 1)) + 2) as remain,
           movies_year,
           movies_rating
    from result_movies_view
    union all
    select movies_id,
           movies_title,
           substring_index(remain, '|', 1),
           substring(remain, char_length(substring_index(remain, '|', 1)) + 2),
           movies_year,
           movies_rating
    from movies_with_parsed_genres
    where char_length(remain) != 0
)
select movies_id, movies_title, genre as movies_genre, movies_year, movies_rating
from movies_with_parsed_genres);