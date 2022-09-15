import sys
import argparse


def get_args():
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--N", type=int, nargs=1, help='return N movies of each genre')
    return parser.parse_args()


def read_from_stdin():
    """
    read from stdin by line
    :return: (genre,title with year inside)
    """
    for line in sys.stdin.readlines():
        key, value = line.split('\t\t')
        yield key, value


def do_reduce(key, value):
    """
    reduce
    :param key: genre
    :param value: title with year inside
    :return: (genre,[title,year]
    """
    genre = key
    year = value[-6:-2]
    value = value.replace(value[-8:], '')
    yield genre, [value[2:-1], year]


def write_to_stdout(key, value):
    """
    write genre,title,year
    :param key: genre
    :param value: [title,year]
    :return: None
    """
    for genre, value in do_reduce(key, value):
        print(genre + ';' + value[0] + ';' + value[1])


def main():
    """
    main function
    :return: None
    """
    args = get_args()
    if args.N is None:
        for genre, value in read_from_stdin():
            write_to_stdout(genre, value)
    else:
        n = args.N[0]
        genre_checker = ''
        counter = 0
        for genre, value in read_from_stdin():
            if genre_checker == genre:
                counter = counter + 1
            else:
                genre_checker = genre
                counter = 1
            if counter <= n:
                write_to_stdout(genre, value)


if __name__ == '__main__':
    main()
