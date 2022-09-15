use movies_db;

drop table if exists lnd_rating;

create table lnd_rating
(
    id           int not null auto_increment,
    user_id      int,
    movie_id     int,
    movie_rating double,
    timestamp    int,
    CONSTRAINT pk_lnd_rating primary key (id)
);