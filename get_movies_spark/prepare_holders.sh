rm -r /tmp/data
mkdir /tmp/data

wget -O /tmp/mt-latest.zip https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip /tmp/mt-latest.zip -d /tmp/data/ 

mkdir /tmp/data/movie
mkdir /tmp/data/rating

mv /tmp/data/ml-latest-small/movies.csv /tmp/data/movie/
mv /tmp/data/ml-latest-small/ratings.csv /tmp/data/rating/

rm -r /tmp/data/ml-latest-small &> /dev/null

hdfs dfs -rm -r /tmp/data &> /dev/null
hdfs dfs -mkdir -p /tmp/data/movie
hdfs dfs -put -f /tmp/data/movie/movies.csv /tmp/data/movie/
hdfs dfs -mkdir -p /tmp/data/rating
hdfs dfs -put -f /tmp/data/rating/ratings.csv /tmp/data/rating/
hdfs dfs -rm -r /tmp/data/output &> /dev/null
