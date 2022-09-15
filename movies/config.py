# filepathes
rating_filepath = 'data/ratings.csv'
movies_filepath = 'data/movies.csv'
bad_data_filepath = 'data/bad_data.txt'

# movie config
movie_year_regex = r'(.+)(\()(\d{4})(\))'
movie_genre_regex = '(no genres listed)'
movie_id_pos = 0
movie_tittle_pos = 1
movie_genre_pos = 2

# rating config
rating_movie_id_pos = 1
rating_pos = 2

# positions after processing files
movie_year_pos = 3
movie_rating_pos = 4

# delimiter
csv_delimiter = ','
