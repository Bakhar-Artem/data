import sys
import argparse
import csv
import re


def get_args():
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--genres", type=str, nargs=1, help='filter movies by genres')
    parser.add_argument("--year_from", type=int, nargs=1, help='filter movies by release year(beginning since)')
    parser.add_argument("--year_to", type=str, nargs=1, help='filter movies by release year(ending on')
    parser.add_argument("--regexp", type=str, nargs=1, help='filter movies by custom regexp')
    return parser.parse_args()


def read_from_stdin():
    """
    read file and yield line
    :return: csv line
    """
    _ = sys.stdin.readline()
    for line in csv.reader(sys.stdin):
        yield line


def write_to_stdout(key, value):
    """
    write map to stdout
    :param key:
    :param value:
    """
    for genre, line in do_map(key, value):
        print(genre + '\t\t' + str(line))


def filter_by_year(title, year_from, year_to):
    """
    filter movies by year
    :param title:
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
    year_pattern = r'\((\d{4})\)$'
    search = re.search(year_pattern, title)
    if search:
        year = int(search.group(1))
        return year_from <= year <= year_to


def filter_by_genre(user_genres, movie_genres):
    """
    filter movies by genre
    :param user_genres:
    :param movie_genres:
    :return:bool
    """

    if movie_genres == '(no genres listed)':
        return False
    if user_genres is None:
        return True
    user_genres_split = user_genres[0].split('|')
    movie_genres_split = movie_genres.split('|')
    for user_genre in user_genres_split:
        if user_genre not in movie_genres_split:
            return False
    return True


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


def do_map(genre, value):
    """
    :param genre: line in next format "genre1|genre2|..."
    :param value: (title with year inside)
    :return: map key= genre, value = [title,year]
    """
    _, title, _ = value
    year_pattern = r'\((\d{4})\)$'
    search = re.search(year_pattern, title)
    year = int(search.group(1))
    title = title.replace(' ' + search.group(0), '')
    yield genre, [title, year]


def main():
    """
    main function
    :return: None
    """
    args = get_args()
    for value in read_from_stdin():
        _, title, genres = value
        if filter_by_year(title, args.year_from, args.year_to) and filter_by_regexp(args.regexp,
                                                                                    title) and filter_by_genre(
            args.genres, genres):
            if args.genres is not None:
                genres = args.genres[0]
            for genre in genres.split('|'):
                write_to_stdout(genre, value)


if __name__ == '__main__':
    main()
