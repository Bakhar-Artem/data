from client_config import *
import argparse
from pymysql import connect


def get_args_from_command_line():
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser(
        "Console utility to find top movies by criteria")
    parser.add_argument("--N", type=int, nargs=1, help='sort movies by rating and genres')
    parser.add_argument("--genres", type=str, nargs=1, help='filter movies by genres')
    parser.add_argument("--year_from", type=int, nargs=1, help='filter movies by release year(beginning since)')
    parser.add_argument("--year_to", type=str, nargs=1, help='filter movies by release year(ending on')
    parser.add_argument("--regexp", type=str, nargs=1, help='filter movies by custom regexp')
    return parser.parse_args()


def get_sp_get_movies_params(args_namespace):
    """
    process args from command line to execute saved procedure
    :param args_namespace: namespace with args from command line
    :return: list [movie_regexp, movie_genres, movie_year_from, movie_year_to, movie_n]
    """
    procedure_params = []
    if args_namespace.regexp:
        procedure_params.append(args_namespace.regexp[0])
    else:
        procedure_params.append(None)
    if args_namespace.genres:
        procedure_params.append(args_namespace.genres[0])
    else:
        procedure_params.append(None)
    if args_namespace.year_from:
        procedure_params.append(int(args_namespace.year_from[0]))
    else:
        procedure_params.append(None)
    if args_namespace.year_to:
        procedure_params.append(args_namespace.year_to[0])
    else:
        procedure_params.append(None)
    if args_namespace.N:
        procedure_params.append(args_namespace.N[0])
    else:
        procedure_params.append(None)
    return procedure_params


if __name__ == '__main__':
    """
    Process command line and execute query on server to get filtered movies
    movie[0] - title
    movie[1] - genre
    movie[2] - year
    movie[3] - rating
    """
    args_namespace = get_args_from_command_line()
    params = get_sp_get_movies_params(args_namespace)
    connection = connect(host=db_host, user=db_user, password=db_password, database=db_name)
    try:
        with connection.cursor() as cursor:
            cursor.callproc(sp_get_movies, params)
            movies = cursor.fetchall()
            for movie in movies:
                print(movie[1] + ', ' + movie[0] + ', ' + movie[2] + ', ' + str(movie[3]))
    finally:
        connection.close()
