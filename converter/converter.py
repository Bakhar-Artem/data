import pandas
import argparse
from pyarrow.parquet import ParquetFile


def csv2parquet(src_file, dst_file):
    """
    Converts csv to parquet
    """
    df = pandas.read_csv(src_file)
    df.to_parquet(dst_file, index=False)


def parquet2csv(src_file, dst_file):
    """
    Converts parquet to csv
    """
    df = pandas.read_parquet(src_file)
    df.to_csv(dst_file, index=False)


def get_parquet_schema(src_file):
    """
    Returns parquet schema
    """
    return ParquetFile(src_file).metadata


def get_args():
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser(
        "Console utility to convert csv to parquet, parquet to csv, get schema of parquet file")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--csv2parquet", type=str, nargs=2, help='convert csv to parquet')
    group.add_argument("--parquet2csv", type=str, nargs=2, help='convert parquet to csv')
    parser.add_argument("--get_schema", nargs=1, help='get schema of parquet')
    return parser.parse_args()


def main():
    args = get_args()
    if args.csv2parquet:
        src_file, dst_file = args.csv2parquet
        csv2parquet(src_file, dst_file)
    elif args.parquet2csv:
        src_file, dst_file = args.parquet2csv
        parquet2csv(src_file, dst_file)
    elif args.get_schema:
        src_file = args.get_schema
        print(get_parquet_schema(src_file))
    else:
        print('Use --help to get help')


if __name__ == '__main__':
    main()
