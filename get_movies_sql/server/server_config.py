# db connection
db_host = 'localhost'
db_user = 'admin'
db_password = 'password'

# ddl scripts list
create_db = ['sql_scripts/ddl/db/movies_db.sql',
             'sql_scripts/ddl/tables/lnd_movies.sql',
             'sql_scripts/ddl/tables/lnd_rating.sql']

# procedure
sp_get_movies = 'sql_scripts/ddl/procedures/sp_get_movies.sql'

# sql inserts
lnd_movies_insert = 'sql_scripts/dml/insert/lnd_movies_insert.sql'
lnd_rating_insert = 'sql_scripts/dml/insert/lnd_rating_insert.sql'

# csv filepath
rating_filepath = 'data/ratings.csv'
movies_filepath = 'data/movies.csv'
bad_data_filepath = 'data/bad_data.txt'

# delimiter
csv_delimiter = ','
query_delimiter = ';'
procedure_delimiter = ';;'

# movie config
movie_year_regex = r'(.+)(\()(\d{4})(\))'

movie_id_pos = 0
movie_title_pos = 1
movie_genre_pos = 2

# rating config
rating_user_id_pos = 0
rating_movie_id_pos = 1
rating_pos = 2
rating_timestamp_pos = 3

# views config
result_view = 'sql_scripts/ddl/views/view_result_movies.sql'
rating_view = 'sql_scripts/ddl/views/view_rating.sql'
movie_view = 'sql_scripts/ddl/views/view_movies.sql'
movie_rating_separated_by_genres_view = 'sql_scripts/ddl/views/view_movies_rating_separated_by_genres.sql'
movie_view_title_regex = '^.+[\\(][0-9]{4}[\\)]$'
movie_view_year_regex = '[0-9]{4}'
movie_view_title_without_year = '[\\(][0-9]{4}[\\)]'
movie_view_genre_regex = '(no genres listed)'
