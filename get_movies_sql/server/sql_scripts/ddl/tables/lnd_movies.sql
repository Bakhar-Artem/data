use movies_db;
drop table if exists lnd_movies;
create table lnd_movies
(
    id            int auto_increment,
    movies_id     int,
    movies_titles varchar(255),
    movies_genre  varchar(255),
    CONSTRAINT pk_lnd_movies primary key (id)
);