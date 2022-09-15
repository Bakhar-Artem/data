reducer="reducer.py"
mapper="mapper.py"
genres="--genres"
regexp="--regexp"
year_from="--year_from"
year_to="--year_to"
number="--N"
while [ -n "$1" ]
do 
if [ "$1" == "$genres" ] || [ "$1" == "$regexp" ] || [ "$1" == "$year_from" ] || [ "$1" == "$year_to" ]
then
        mapper="$mapper $1"
        shift
        mapper="$mapper $1"
elif [ "$1" == "$number" ]
        then
        reducer="$reducer $1"
        shift
        reducer="$reducer $1"
fi
shift
done

hdfs dfs -rm /tmp/output/* 2>>log.txt
hdfs dfs -rmdir /tmp/output 2>>log.txt

yarn jar /usr/lib/hadoop/hadoop-streaming.jar -input /tmp/data/movies.csv -output /tmp/output -file ~/mapper.py -file ~/reducer.py -mapper "python3 $mapper" -reducer "python $reducer"
