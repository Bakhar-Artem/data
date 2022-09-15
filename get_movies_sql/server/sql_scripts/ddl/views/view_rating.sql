use movies_db;
drop view if exists rating_view;
create view rating_view as
select movie_id, AVG(movie_rating) as movie_rating
from lnd_rating
group by movie_id;