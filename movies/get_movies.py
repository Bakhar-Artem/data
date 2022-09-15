import config
import csv
import re
import functools
import argparse


def get_movies_list(movies_csv_filepath):
    """
    Function to read movies from csv
    :param movies_csv_filepath:
    :return:list of movies from csv
    """
    bad_data_file = open(config.bad_data_filepath, 'w')
    movies_list = []
    with open(movies_csv_filepath, 'r', newline='') as csv_file:
        reader = csv.reader(csv_file, delimiter=config.csv_delimiter)
        for row in reader:
            matcher = re.match(config.movie_year_regex, row[config.movie_tittle_pos])
            if matcher and (row[config.movie_genre_pos] != config.movie_genre_regex):
                movies_list.append([row[config.movie_id_pos], row[config.movie_tittle_pos],
                                    row[config.movie_genre_pos], matcher.group(3)])
            else:
                bad_data_file.write(config.csv_delimiter.join(row))
                bad_data_file.write('\n')
    bad_data_file.close()
    return movies_list


def get_movie_rating_dict(rating_csv_filepath):
    """
    Function to generate rating dictionary
    :param rating_csv_filepath:
    :return: dictionary (movie_id,movie_rating)
    """
    rating_dict = dict()
    counter_dict = dict()
    movies_rating_dict = dict()
    with open(rating_csv_filepath, 'r', newline='') as csv_file:
        _ = csv_file.readline()  # skip description of csv file
        reader = csv.reader(csv_file, delimiter=config.csv_delimiter)
        for row in reader:
            if rating_dict.get(row[config.rating_movie_id_pos]):  # get() used to check is None
                rating_dict[row[config.rating_movie_id_pos]] += float(row[config.rating_pos])
                counter_dict[row[config.rating_movie_id_pos]] += 1
            else:
                rating_dict[row[config.rating_movie_id_pos]] = float(row[config.rating_pos])
                counter_dict[row[config.rating_movie_id_pos]] = 1
        for key in rating_dict.keys():
            movies_rating_dict[key] = round(rating_dict[key] / counter_dict[key], 4)
    return movies_rating_dict


def get_movie_list_with_rating(movies_list, rating_dict):
    """
    Function to join list of movies from csv and rating dictionary from csv
    :param movies_list:
    :param rating_dict:
    :return: list of movies with ratings
    """
    bad_data_file = open(config.bad_data_filepath, 'a')
    movie_list_copy = movies_list.copy()
    movies_list_with_rating = []

    for row in movie_list_copy:
        try:
            row.append(rating_dict[row[config.movie_id_pos]])
            movies_list_with_rating.append(row)
        except KeyError:
            bad_data_file.write(config.csv_delimiter.join(row))
            bad_data_file.write('\n')
    bad_data_file.close()
    return movies_list_with_rating


def get_movies_list_filtered_by_regex(regexp, movie_list):
    """
    Function to filter movies by custom regexp
    :param regexp:
    :param movie_list:
    :return: list of movies filtered by regexp
    """
    movies_list_with_regex = []
    for movie in movie_list:
        match = re.search(regexp, movie[config.movie_tittle_pos])
        if match:
            movies_list_with_regex.append(movie)
    return movies_list_with_regex


def get_movies_list_filtered_by_year(movie_list, start=0, finish=9999):
    """
    Function to filter movies by release year
    If start or finish values are not passed function will use default parameters
    :param movie_list:
    :param start:
    :param finish:
    :return: list of movies filtered by release year
    """
    movies_list_with_year = []
    for movie in movie_list:
        if start <= int(movie[config.movie_year_pos]) <= finish:
            movies_list_with_year.append(movie)
    return movies_list_with_year


def get_movies_list_filtered_by_genres(movie_list, genres_list):
    """
    Function to filter movies by genres
    :param movie_list:
    :param genres_list:
    :return:
    """
    movies_list_with_genres = []
    for movie in movie_list:
        match = True
        for genre in genres_list:
            if genre not in movie[config.movie_genre_pos]:
                match = False
                break
        if match:
            movies_list_with_genres.append(movie)
    return movies_list_with_genres


def compare_rating(first_movie, second_movie):
    """
    Helper that help sort list of lists by rating
    :param first_movie:
    :param second_movie:
    :return:
    """
    return first_movie[config.movie_rating_pos] - second_movie[config.movie_rating_pos]


def get_movies_list_sorted_by_rating_and_genres(movies_list, genres_list, n=-1):
    """
    Function to sort list of movies by rating that have listed genre
    :param movies_list:
    :param genres_list:
    :param n:
    :return: n movies of each listed genre, if n are not passed function will return all movies with listed genre
    """
    movies_list_sorted_by_rating_and_genres = []
    movies_list.sort(key=functools.cmp_to_key(compare_rating), reverse=True)
    for genre in genres_list:
        i = 0
        for movie in movies_list:
            if genre in movie[config.movie_genre_pos]:
                movie_copy = movie.copy()
                movie_copy[config.movie_genre_pos] = genre
                movies_list_sorted_by_rating_and_genres.append(movie_copy)
                i += 1
            if i == n:
                break
    return movies_list_sorted_by_rating_and_genres


def get_genres_list(movies_list):
    """
    Helper that generate list of unique genres
    :param movies_list:
    :return: genres_list
    """
    genres_set = set()
    for movie in movies_list:
        genres_list = movie[config.movie_genre_pos].split(config.movie_genre_separator)
        for genre in genres_list:
            genres_set.add(genre)
    genres_list = list(genres_set)
    genres_list.sort()
    return genres_list


def get_args():
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


if __name__ == '__main__':
    """
    Get movies from csv and join ratings from another csv.
    Process command line and print movies filtered by filters
    """
    movies = get_movie_list_with_rating(get_movies_list(config.movies_filepath),
                                        get_movie_rating_dict(config.rating_filepath))
    genres = []
    args = get_args()
    if args.regexp:
        movies = get_movies_list_filtered_by_regex(args.regexp[0], movies)
    if args.year_to and args.year_from:
        movies = get_movies_list_filtered_by_year(movies, int(args.year_from[0]), int(args.year_to[0]))
    elif args.year_to:
        movies = get_movies_list_filtered_by_year(movies, finish=int(args.year_to[0]))
    elif args.year_from:
        movies = get_movies_list_filtered_by_year(movies, start=int(args.year_from[0]))
    if args.genres:
        genres = args.genres[0].split('|')
        movies = get_movies_list_filtered_by_genres(movies, genres)
    if args.N:
        if genres.__len__() == 0:
            genres = get_genres_list(movies)
        movies = get_movies_list_sorted_by_rating_and_genres(movies, genres, args.N[0])
    if not (args.N or args.genres or args.year_from or args.year_to or args.regexp):
        movies = get_movies_list_sorted_by_rating_and_genres(movies, genres_list=get_genres_list(movies))

    print('genre,tittle,year,rating')
    for movie in movies:
        print(movie[config.movie_genre_pos] + ', ' + movie[config.movie_tittle_pos] + ', ' + movie[
            config.movie_year_pos] + ', ' + str(movie[config.movie_rating_pos]))
