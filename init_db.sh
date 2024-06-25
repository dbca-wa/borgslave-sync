#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "Begin to initialize the database"
host=${GEOSERVER_PGSQL_HOST:-localhost}
port=${GEOSERVER_PGSQL_PORT:-5432}
db=${GEOSERVER_PGSQL_DATABASE:-kmi}
dbuser=${GEOSERVER_PGSQL_USERNAME:-oim}
dbpassword=${GEOSERVER_PGSQL_PASSWORD}
admin=postgres
adminpassword=${GEOSERVER_PGSQL_ADMINPASSWORD}
admindb=postgres
declare -a postgis_extensions=(
[0]=postgis
[1]=postgis_raster
[2]=postgis_topology
)

#check whether data exists or not
#return 0, data exists
#return 1, data doesn't exists
#return 2, error
function has_data() {
    if [[ "$3" == "" ]]; then
        if [[ "$1" == "${admindb}" ]]; then
            user=${admin}
        else
            user=${dbuser}
        fi
    else
        user=$3
    fi
    if [[ "${user}" == "${admin}" ]]; then
        if [[ "${adminpassword}" == "" ]]; then
            result=$(psql -h ${host} -p ${port} -d $1 -U ${admin} -w -c "$2")
        else
            result=$(export PGPASSWORD=${adminpassword} ; psql -h ${host} -p ${port} -d $1 -U ${admin} -w -c "$2")
        fi
    else
        if [[ "${dbpassword}" == "" ]]; then
            result=$(psql -h ${host} -p ${port} -d $1 -U ${dbuser} -w -c "$2")
        else
            result=$(export PGPASSWORD=${dbpassword} ; psql -h ${host} -p ${port} -d $1 -U ${dbuser} -w -c "$2")
        fi
    fi
    if [[ $? -ne 0 ]]; then
        echo "Failed to execute the sql '$2' "
        return 2
    fi
    rows=$(echo "$result" | sed -n '3p' | sed 's/^[ \t]*//;s/[ \t]*$//')
    if [[ $rows -eq 0 ]]; then
        return 1
    else
        return 0
    fi
}

#execute sql
#return 0, succeed
#return 1, error
function execute_sql() {
    if [[ "$3" == "" ]]; then
        if [[ "$1" == "${admindb}" ]]; then
            user=${admin}
        else
            user=${dbuser}
        fi
    else
        user=$3
    fi
    if [[ "${user}" == "${admin}" ]]; then
        if [[ "${adminpassword}" == "" ]]; then
            psql -h ${host} -p ${port} -d $1 -U ${admin} -w -c "$2"
        else
            export PGPASSWORD=${adminpassword} ; psql -h ${host} -p ${port} -d $1 -U ${admin} -w -c "$2"
        fi
    else
        if [[ "${password}" == "" ]]; then
            psql -h ${host} -p ${port} -d $1 -U ${dbuser} -w -c "$2"
        else
            export PGPASSWORD=${dbpassword} ; psql -h ${host} -p ${port} -d $1 -U ${dbuser} -w -c "$2"
        fi
    fi
    if [[ $? -ne 0 ]]; then
        echo "Failed to execute the sql '$2' "
        return 1
    else
        return 0
    fi
}
#wait until the database is available
result=1
while [[ ${result} -ne 0 ]]; do
    echo "check whether the database is available"
    execute_sql ${admindb} "select now();"
    result=$?
    echo "=========${result}"
    if [[ ${result} -ne 0 ]]; then
        echo "Database(${host}:${port}/${admindb}) is not available,wait 60 seconds and check again"
        sleep 60
    fi
done
echo "Database(${host}:${port}/${admindb}) is available"

#check whether the database exists
has_data ${admindb} "select count(1) from pg_database where datname='${db}';"
result=$?
if [[ $result -eq 2 ]]; then
    #failed
    exit 1
elif [[ $result -eq 1 ]]; then
    #db does not exist, try to create it
    echo "The database(${db}) doesn't exist,try to create it."
    #check whether user exists or not
    #if doesn't exist, try to create it.
    has_data ${admindb} "select count(1) from pg_roles where rolname='${dbuser}';"
    result=$?
    if [[ $result -eq 2 ]]; then
        #failed
        exit 1
    elif [[ $result -eq 1 ]]; then
        #role does not exist, try to create it
        echo "The role(${dbuser}) doesn't exist,try to create it."
        if [[ "${dbpassword}" == "" ]]; then
            execute_sql ${admindb} "CREATE USER ${dbuser} WITH NOCREATEDB CREATEROLE;"
        else
            execute_sql ${admindb} "CREATE USER ${dbuser} WITH PASSWORD '${dbpassword}' NOCREATEDB CREATEROLE;"
        fi
        result=$?
        if [[ $result -ne 0 ]]; then
            #failed
            exit 1
        fi
        echo "The role(${dbuser}) was created."
    else
        echo "The role(${dbuser}) exists"
    fi
    #create the db
    echo "Begin to create the database(${db})"
    execute_sql ${admindb} "CREATE DATABASE ${db} WITH OWNER=${dbuser}"
    result=$?
    if [[ $result -ne 0 ]]; then
        #failed
        exit 1
    fi
    echo "The database(${db}) was created"

else
    echo "The database(${db}) exists"
fi

for extension in ${postgis_extensions[@]}
do
    #Check whether the postgis extension exist or not
    has_data ${db} "select count(1) from pg_extension where extname='${extension}';" ${admin}
    result=$?
    if [[ $result -eq 2 ]]; then
        #failed
        exit 1
    elif [[ $result -eq 1 ]]; then
        #The extension doesn't exist
        echo "The extension(${extension}) doesn't exists, try to create it"
        execute_sql ${db} "CREATE EXTENSION IF NOT EXISTS ${extension};" ${admin}
        result=$?
        if [[ $result -ne 0 ]]; then
            #failed
            exit 1
        fi
        echo "The extension(${extension}) was created"
    else
        echo "The extension(${extension}) exists"
    fi
done
