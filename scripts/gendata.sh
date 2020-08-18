#!/bin/bash


if [ $# -eq 0 ]
  then
    echo "No mode or s3 bucket provided. gendata.sh [--oc | --docker] <s3bucket>"
    exit 1
fi

SCALE=1000 # Scale factor

oc_sql_exec() {
    oc exec -it presto-coordinator-0 presto-cli -- --server presto:8080 --catalog hive --execute "$1"
}

docker_sql_exec() {
    docker exec -it presto presto-cli --catalog hive --execute "$1"
}


if [[ $1 = "--oc" ]]
then
  declare TABLES="$(oc_sql_exec "SHOW TABLES FROM tpcds.sf1;" | sed s/\"//g)"

  oc_sql_exec "CREATE SCHEMA hive.tpcds WITH (location = 's3a://$1/');"
  for tab in $TABLES; do
    oc_sql_exec "CREATE TABLE tpcds.$tab AS SELECT * FROM tpcds.sf$SCALE.$tab;"
  done

else
  declare TABLES="$(docker_sql_exec "SHOW TABLES FROM tpcds.sf1;" | sed s/\"//g)"

  docker_sql_exec "CREATE SCHEMA hive.tpcds WITH (location = 's3a://$1/');"
  for tab in $TABLES; do
    docker_sql_exec "CREATE TABLE tpcds.$tab AS SELECT * FROM tpcds.sf$SCALE.$tab;"
  done
fi
