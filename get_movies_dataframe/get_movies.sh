genres="--genres"
regexp="--regexp"
year_from="--year_from"
year_to="--year_to"
number="--N"
args=""
while [ -n "$1" ]
do 
if [ "$1" == "$genres" ] || [ "$1" == "$regexp" ] || [ "$1" == "$year_from" ] || [ "$1" == "$year_to" || "$1" == "$number" ]
then
	args="$args $1"
	shift
	args="$args $1"

fi
shift
done


spark-submit get_movies_df_cluster.py $args

hdfs dfs -cat hdfs:/tmp/data/output/*
