import re
import argparse
from pyspark import SparkContext
import csv


def get_args():
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--N", type=int, nargs=1, help='sort movies by rating and genres')
    parser.add_argument("--genres", type=str, nargs=1, help='filter movies by genres')
    parser.add_argument("--year_from", type=int, nargs=1, help='filter movies by release year(beginning since)')
    parser.add_argument("--year_to", type=str, nargs=1, help='filter movies by release year(ending on')
    parser.add_argument("--regexp", type=str, nargs=1, help='filter movies by custom regexp')
    return parser.parse_args()


def normalize_movie_line(movie_line):
    """
    process movie to normal
    :param movie_line:
    :return:
    """
    reader = csv.reader([movie_line])
    movie_id, title, genres = next(reader)
    title_year_regexp = r'(.*)[ ]\((\d{4})\)$'
    matcher = re.match(title_year_regexp, title)
    if matcher:
        title = matcher.group(1)
        year = matcher.group(2)
    else:
        title = ''
        year = 0
    genres = genres.split('|')
    return [(int(movie_id), (title, year, genre)) for genre in genres]


def normalize_rating_line(rating_line):
    """
    process rating line to normal
    :param rating_line:
    :return:
    """
    reader = csv.reader([rating_line])
    _, movie_id, rating, _ = next(reader)
    return int(movie_id), [float(rating), 1]


def filter_by_year(year, year_from, year_to):
    """
    filter movies by year
    :param year:
    :param year_from:
    :param year_to:
    :return: bool
    """
    if year_from is None:
        year_from = 0
    else:
        year_from = int(year_from[0])
    if year_to is None:
        year_to = 9999
    else:
        year_to = int(year_to[0])
    return year_from <= int(year) <= year_to


def filter_by_genre(user_genres, movie_genre):
    """
    filter movies by genre
    :param user_genres:
    :param movie_genre:
    :return:bool
    """

    if movie_genre == '(no genres listed)':
        return False
    if user_genres is None:
        return True
    user_genres_split = user_genres[0].split('|')
    for user_genre in user_genres_split:
        if user_genre == movie_genre:
            return True
    return False


def filter_by_regexp(regexp, title):
    """
    filter movie by user regexp
    :param regexp: user regexp
    :param title: movie title
    :return:  Search to check is match regexp
    """
    if regexp is None:
        return True
    search = re.search(regexp[0], title)
    return search


def filter_movie(movie_line, args):
    _, movie = movie_line
    title, year, genre = movie
    return filter_by_regexp(args.regexp, title) and filter_by_genre(args.genres, genre) and filter_by_year(year,
                                                                                                           args.year_from,
                                                                                                           args.year_to)


def read_csv(csv_path):
    data_rdd = sc.textFile(csv_path)
    header = data_rdd.first()
    data_rdd = data_rdd.filter(lambda line: line != header)
    return data_rdd


def get_movies_rdd(path, args):
    movies_rdd = read_csv(path)
    movies_rdd = movies_rdd.flatMap(normalize_movie_line).filter(
        lambda movie_line: filter_movie(movie_line, args))
    return movies_rdd


def get_rating_rdd(path):
    rating_rdd = read_csv(path)
    rating_rdd = rating_rdd.map(normalize_rating_line).reduceByKey(
        lambda r1, r2: [r1[0] + r2[0], r1[1] + r2[1]]).mapValues(
        lambda values: values[0] / values[1])
    return rating_rdd


def get_movies_rating_rdd(movies_rdd, rating_rdd, N):
    if N:
        n = N[0]
        movies_rating_rdd = movies_rdd.join(rating_rdd).mapValues(
            lambda values: (values[0][0], values[0][1], values[0][2], values[1])).sortBy(
            lambda line: (line[1][3]), ascending=False).groupBy(lambda line: line[1][2]).flatMap(
            lambda line: list(line[1])[:n]).map(
            lambda line: (line[1][2], [line[1][0], line[1][1], line[1][3]]))
    else:
        movies_rating_rdd = movies_rdd.join(rating_rdd).mapValues(
            lambda values: (values[0][0], values[0][1], values[0][2], values[1])).sortBy(
            lambda line: (line[1][3]), ascending=False).groupBy(lambda line: line[1][2]).flatMap(
            lambda line: list(line[1])[0:]).map(
            lambda line: {line[1][2]: [line[1][0], line[1][1], line[1][3]]})

    return movies_rating_rdd


def main():
    args = get_args()

    movies_rdd = get_movies_rdd('data/movies.csv', args)
    rating_rdd = get_rating_rdd('data/ratings.csv')
    movies_rating_rdd = get_movies_rating_rdd(movies_rdd, rating_rdd, args.N)
    movies_rating_rdd.saveAsTextFile('data/output')


if __name__ == '__main__':
    sc = SparkContext('local[*]', 'get_movies')
    main()
