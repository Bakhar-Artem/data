import argparse
import re
from pyspark.sql.functions import split, explode, avg
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Get Movies").getOrCreate()


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


def read_csv(movie_path, rating_path):
    """
    Create dataframes from csv files
    :param movie_path: movies.csv path
    :param rating_path: ratings.csv path
    :return: list of dataframes
    """
    movies_df = spark.read.format('csv').option('header', 'true').load(movie_path)
    rating_df = spark.read.format('csv').option('header', 'true').load(rating_path)
    return movies_df, rating_df


def normalize_movie_df(movies_df, title_regexp):
    """
    normalize movie_df
    :param movies_df: movie dataframe
    :param title_regexp: movie title regexp
    :return: normalized movie dataframe
    """
    get_title = spark.udf.register("get_title", lambda title: re.search('^(.+)[ ]+\((\d{4})\)$', title).group(1))
    get_year = spark.udf.register("get_year", lambda title: re.search('^(.+)[ ]+\((\d{4})\)$', title).group(2))
    movies_filt_title = movies_df.filter(movies_df.title.rlike(title_regexp))
    movies_df_normalized = movies_filt_title.select('movieId', get_title('title').alias('title'),
                                                    get_year('title').alias('year'),
                                                    explode(split('genres', '\\|')).alias('genre'))
    return movies_df_normalized


def normalize_rating_df(rating_df):
    """
    normalized rating dataframe
    :param rating_df: rating dataframe
    :return: normalized rating dataframe
    """
    ratings_df_average = rating_df.groupBy('movieId').agg(avg('rating').alias('rating'))
    return ratings_df_average


def movie_rating_df_join(movies_df_normalized, ratings_df_average):
    """
    create dataframe on join movie dataframe on rating dataframe
    :param movies_df_normalized: normalized movie dataframe
    :param ratings_df_average: normalized rating dataframe
    :return: movie with rating dataframe
    """
    movie_rating_df = movies_df_normalized.join(ratings_df_average,
                                                movies_df_normalized.movieId == ratings_df_average.movieId, 'inner')
    movie_rating_df = movie_rating_df.sort(movie_rating_df.rating.desc(), movie_rating_df.genre.asc())
    return movie_rating_df


def prepare_args(args):
    """
    process command line arguments
    :param args: namespace
    :return: list [year_from,year_to,regexp,genres,N]
    """
    if args.year_from:
        year_from = args.year_from[0]
    else:
        year_from = 0
    if args.year_to:
        year_to = args.year_to[0]
    else:
        year_to = 9999
    if args.genres:
        genres = args.genres[0].split('|')
    else:
        genres = []
    if args.regexp:
        regexp = args.regexp[0]
    else:
        regexp = ''
    if args.N:
        n = args.N[0]
    else:
        n = -1
    return [year_from, year_to, regexp, genres, n]


def filter_movie_rating_df(movie_rating_df, prepared_args):
    """
    filter movie with rating dataframe with prepared args from command line
    :param movie_rating_df: movie with rating dataframe
    :param prepared_args: list of args
    :return: filtered movie with rating dataframe
    """
    year_from, year_to, regexp, genres, n = prepared_args
    movie_rating_df = movie_rating_df.withColumn('year', movie_rating_df.year.cast('int'))
    if genres and regexp:
        movie_rating_filtered_df = movie_rating_df.filter(
            (year_from <= movie_rating_df.year) & (year_to >= movie_rating_df.year) & (
                movie_rating_df.title.rlike(regexp)) & movie_rating_df.genre.isin(genres)).select('genre', 'title',
                                                                                                  'year',
                                                                                                  'rating')
    elif genres and not regexp:
        movie_rating_filtered_df = movie_rating_df.filter(
            (year_from <= movie_rating_df.year) & (year_to >= movie_rating_df.year) & movie_rating_df.genre.isin(
                genres)).select('genre', 'title',
                                'year',
                                'rating')
    elif not genres and regexp:
        movie_rating_filtered_df = movie_rating_df.filter(
            (year_from <= movie_rating_df.year) & (year_to >= movie_rating_df.year) & (
                movie_rating_df.title.rlike(regexp))).select('genre', 'title',
                                                             'year',
                                                             'rating')
    else:
        movie_rating_filtered_df = movie_rating_df.filter(
            (year_from <= movie_rating_df.year) & (year_to >= movie_rating_df.year)).select('genre', 'title',
                                                                                            'year',
                                                                                            'rating')
    return movie_rating_filtered_df


def reduce_df(movie_rating_df, n, genres, path):
    if n != -1:
        emp_rdd = spark.sparkContext.emptyRDD()

        columns1 = StructType([StructField('genre', StringType(), True),
                               StructField('title', StringType(), True),
                               StructField('year', IntegerType(), True),
                               StructField('rating', DoubleType(), True)])

        result_df = spark.createDataFrame(data=emp_rdd,
                                          schema=columns1)
        if genres:
            for user_genre in genres:
                result_df = result_df.union(movie_rating_df.filter(movie_rating_df.genre == user_genre).limit(n))
            result_df.write.format('csv').option('header', 'false').option('delimiter', ',').csv(path)
        else:
            genres_df = movie_rating_df.select('genre').alias('genre').distinct().collect()
            genres_list = [genre['genre'] for genre in genres_df]
            for genre in genres_list:
                result_df = result_df.union(movie_rating_df.filter(movie_rating_df.genre == genre).limit(n))
            result_df.write.format('csv').option('header', 'false').option('delimiter', ',').csv(path)
    else:
        movie_rating_df.write.format('csv').option('header', 'false').option('delimiter', ',').csv(path)


def main():
    args = get_args()
    prepared_args = prepare_args(args)
    movies_df, ratings_df = read_csv('data/movies.csv', 'data/ratings.csv')
    title_regexp = r'^(.+)[ ]+\((\d{4})\)$'
    movies_df = normalize_movie_df(movies_df, title_regexp)
    ratings_df = normalize_rating_df(ratings_df)
    movie_rating_df = movie_rating_df_join(movies_df, ratings_df)
    movie_rating_df = filter_movie_rating_df(movie_rating_df, prepared_args)
    _, _, _, genres, n = prepared_args
    reduce_df(movie_rating_df, n, genres, 'data/output')


if __name__ == '__main__':
    main()
