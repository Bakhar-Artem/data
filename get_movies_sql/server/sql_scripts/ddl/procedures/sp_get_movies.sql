use movies_db;;
drop procedure if exists sp_get_movies;;
create definer=`admin`@`localhost` procedure `sp_get_movies`(in rgx varchar(100), in genres varchar(100), in year_from int, in year_to int, in n int)
begin
    with recursive parsed_genres as (
        select genres as remain, substring_index(genres, '|', 1) as genre
         union all
        select substring(remain, char_length(genre) + 2),
               substring_index(substring( remain, char_length(genre) + 2 ), '|', 1)
          from parsed_genres
         where char_length(remain) > char_length(genre)
),  filtered_movies_ratings as (
        select  movies_title,
				movies_genre,
                movies_year,
                movies_rating,
               rank() over (partition by movies_genre order by movies_rating DESC, movies_year DESC, movies_title) as rnk
          from movies_and_rating_seperated_genres
         where (rgx is null or movies_title regexp rgx)
               and (genres is null or movies_genre in (select genre from parsed_genres))
               and (year_from is null or year_from <= movies_year)
               and (year_to is null or year_to >= movies_year)
)
    select  movies_title,movies_genre, movies_year, movies_rating
      from filtered_movies_ratings
     where rnk <= if(n is null or n < 1, ~0, n);
end;;