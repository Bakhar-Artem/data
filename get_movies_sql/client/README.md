# Console utility to find top movies by criteria

**usage** python3 get_movies.py [OPTION] [VALUE]

# Features

| Command     | Description                                            | Example                                                   |
|:------------|:-------------------------------------------------------|:----------------------------------------------------------|
| --N         | get top N movies of each genre                         | --N 4                                                     |
| --genres    | get movies that have listed genres                     | --genres Action, --genres 'Action**delimiter***Adventure' |
| --year_from | get movies that were released at this year and older   | --year_from 2014                                          |
| --year_to   | get movies that were released at this year and younger | --year_to 2020                                            |
| --regexp    | get movies that match custom regular expression        | --regexp 'some regexp'                                    |

**delimiter* = |

**If not parameter is passed utility will print movies sorted by rating foreach genre**

# Config

To change database properties you can use config file *client_config.py*

# Run

Before run you need to download extra lib

pip install -r requirements.txt
