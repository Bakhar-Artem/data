# Console utility to convert csv to parquet, parquet to csv, get schema of parquet file

*usage* python3 converter.py [OPTION] [FILENAME]

# Features
| Command       | Description            | Example                                     |
|:--------------|:-----------------------|:--------------------------------------------|
| --csv2parquet | convert csv to parquet | --csv2parquet src_file.csv dst_file.parquet |
| --parquet2csv | convert parquet to csv | --parquet2csv src_file.parquet dst_file.csv |
| --get_schema  | get schema of parquet  | --get_schema src_file.parquet               |
| -h, --help    | get utility info       | --help                                      |

# Run
Before run you need download extra libs

***pip install -r requirements.txt***