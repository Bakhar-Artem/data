from pymysql import connect, Error
from server_config import *
import csv


def fill_lnd_movies(movies_csv_filepath):
    """
    fill table lnd_movies with original data
    :param movies_csv_filepath: csv_filepath with original data
    """
    connection = connect(host=db_host, user=db_user, password=db_password)
    cursor = connection.cursor()
    with open(lnd_movies_insert) as script:
        queries = script.read().split(query_delimiter)
        queries.remove('')
        cursor.execute(queries[0])
    try:
        with open(movies_csv_filepath, 'r', newline='') as csv_file:
            _ = csv_file.readline()
            reader = csv.reader(csv_file, delimiter=csv_delimiter)
            for row in reader:
                id = row[movie_id_pos]
                title_year = row[movie_title_pos]
                genre = row[movie_genre_pos]
                cursor.execute(queries[1], (id, title_year, genre))
    except Error as e:
        print(e)
    finally:
        cursor.close()
        connection.commit()
        connection.close()


def fill_lnd_rating(rating_csv_filepath):
    """
    fill table lnd_rating with original data
    :param rating_csv_filepath: csv_filepath with original data
    """
    connection = connect(host=db_host, user=db_user, password=db_password)
    cursor = connection.cursor()
    try:
        with open(lnd_rating_insert) as script:
            queries = script.read().split(query_delimiter)
            queries.remove('')
            cursor.execute(queries[0])
        with open(rating_csv_filepath, 'r', newline='') as csv_file:
            _ = csv_file.readline()  # skip description of csv file
            reader = csv.reader(csv_file, delimiter=csv_delimiter)
            for row in reader:
                user_id = row[rating_user_id_pos]
                movie_id = row[rating_movie_id_pos]
                rating = row[rating_pos]
                timestamp = row[rating_timestamp_pos]
                cursor.execute(queries[1], (user_id, movie_id, rating, timestamp))
    except Error as e:
        print(e)
    finally:
        cursor.close()
        connection.commit()
        connection.close()


def create_database():
    """
    create database
    if database exists script will drop and create it again
    """
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            for file in create_db:
                sql_file = open(file).read()
                queries = sql_file.split(query_delimiter)
                queries.remove('')
                for query in queries:
                    cursor.execute(query)
            cursor.close()
    finally:
        connection.close()


def create_movie_view():
    """
    create view with processed data from lnd_movies table
    """
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            with open(movie_view) as script:
                queries = script.read().split(query_delimiter)
                queries.remove('')
                cursor.execute(queries[0])
                cursor.execute(queries[1])
                cursor.execute(queries[2],
                               (movie_view_title_without_year, movie_view_year_regex, movie_view_title_regex,
                                movie_view_genre_regex))
                cursor.close()
    finally:
        connection.commit()
        connection.close()


def create_rating_view():
    """
    create view with processed data from lnd_rating table
    """
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            with open(rating_view) as script:
                queries = script.read().split(query_delimiter)
                queries.remove('')
                cursor.execute(queries[0])
                cursor.execute(queries[1])
                cursor.execute(queries[2])
                cursor.close()
    finally:
        connection.commit()
        connection.close()


def create_result_movies_view():
    """
    create view with combined data from movies and rating views
    """
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            with open(result_view) as script:
                queries = script.read().split(query_delimiter)
                queries.remove('')
                cursor.execute(queries[0])
                cursor.execute(queries[1])
                cursor.execute(queries[2])
                cursor.close()
    finally:
        connection.commit()
        connection.close()


def create_get_movies_procedure():
    """
    create saved procedure to get filtered movies
    """
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            with open(sp_get_movies) as script:
                queries = script.read().split(procedure_delimiter)
                queries.remove('')
                cursor.execute(queries[0])
                cursor.execute(queries[1])
                cursor.execute(queries[2])
                cursor.close
    finally:
        connection.close()


def create_movie_rating_separated_by_genres_view():
    """
    create view with movies and rating separated by genres
    """
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            with open(movie_rating_separated_by_genres_view) as script:
                queries = script.read().split(query_delimiter)
                queries.remove('')
                for query in queries:
                    cursor.execute(query)
                cursor.close
    finally:
        connection.close()


if __name__ == '__main__':
    """
    run sql scripts to setup sql server
    """
    create_database()
    fill_lnd_movies(movies_filepath)
    fill_lnd_rating(rating_filepath)
    create_movie_view()
    create_rating_view()
    create_result_movies_view()
    create_movie_rating_separated_by_genres_view()
    create_get_movies_procedure()
