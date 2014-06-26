#!/bin/bash

usage() { echo "Usage: $0 -t <tenant> -o <operation = downloads3|importfroms3file|exportfrommongo|importtosql|all> [-c <columnfile>] [-s <schema>] [-u uat] " 1>&2; exit 1; }
log() { now=`date`; echo "[${now}] ${1} "; }
checkmongo() {
    # start mongod, if it isnt running
    mongostat=`ps -eaf | grep mongod | grep -v grep|wc -l`
    if [[ ${mongostat} != "1" ]]; then 
        mongostart
    fi
}

# Get the command line arg
while getopts ":t:o:c:s:u:" arg; do
    case "${arg}" in
        t) tenant=${OPTARG} ;;
        o) operation=${OPTARG} ;;
        c) columnfile=${OPTARG} ;;
        s) schema=${OPTARG} ;;
        u) uat=${OPTARG} ;;
        *) usage ;;
    esac
done
shift $((OPTIND-1))

# Globals
tenant=`echo ${tenant} | tr '[:upper:]' '[:lower:]'`

if [[ "$operation" == "" ]]; then
   operation='all'
fi

if [[ "$tenant" == "" ]]; then
    usage;
fi

if [[ "$schema" == "" ]]; then
    schema="${tenant}"
fi

raw_file_path="/storage/data/tenant/${tenant}/${schema}/raw_data"
sql_file_path="/storage/data/tenant/${tenant}/${schema}/sql_data"

base_path=`pwd`
mongo_scripts="../../../mongo_scripts/common"
sql_scripts="../sql"

log "Initializing from map files ... "
. ./readmap.sh 'downloadAllOpps.default.map' "${columnfile}"
log "Completed Initializing"

##############################################################################################################################
# Step 1: Download from S3
##############################################################################################################################
if [[ ${operation} == "downloads3" || ${operation} == "all" ]]; then

    # Prep directories
    rm -rf "${raw_file_path}"
    mkdir -p "${raw_file_path}"
    s3path="s3://ds-mongodump"
    if [[ "${uat}" ]]; then s3path="$s3path/uat"; fi

    s3_file_nm=`s3cmd ls ${s3path}/${tenant}/ | sort | grep .tar | tail -1 | rev | awk -F"/" '{ print $1 }' | rev`

    # download
    log "STARTED download of S3 file ${s3path}/${tenant}/${s3_file_nm} ..."
    if [[ ${s3_file_nm} ]]; then
        s3cmd get "${s3path}/${tenant}/${s3_file_nm}" "${raw_file_path}/."
        if [[ $? -ne 0 ]]; then log "Error downloading file"; exit 1; fi
    else
        log "No files to export from S3 folder!! Exiting Script"
        exit 1;
    fi

    # unzip
    cd "${raw_file_path}"
    tar -xf "${s3_file_nm}" -C "${raw_file_path}" --strip-components=1; 
    if [[ $? -ne 0 ]]; then log "Error unzipping file ${s3_file_nm}"; exit 1; fi

    rm *.tar
    gunzip ${raw_file_path}/export/*.json.gz
    mv ${raw_file_path}/export/* "${raw_file_path}/"

    if [[ $? -ne 0 ]]; then log "Error in moving files from s3 for ${raw_file_path}/"; exit 1; fi

    log "COMPLETED download of S3 file to ${raw_file_path}/"
    log
fi

##############################################################################################################################
# Step 2: Import prod mongo export file downloaded from S3 into local mongo
##############################################################################################################################
if [[ ${operation} == "importfroms3file" || ${operation} == "all" ]]; then

    # drop existing database
    log "STARTED import of entities to local mongo ..."
    checkmongo;
    mongo --quiet ${tenant} --eval "db.dropDatabase();"

    for coll in "${objs[@]}"
    do
        if [[ ${coll} == "contacts" ]]; then fullCollName="core.${coll}"; else fullCollName="app.${coll}"; fi
        log "Importing ${coll} ..."
        mongoimport --quiet --db "${tenant}" --collection "app.${coll}" --type json --file "${raw_file_path}/${fullCollName}.json"
        if [[ $? -ne 0 ]]; then log "Error in importing data into Mongo for tenant ${tenant} and app.${coll}"; exit 1; fi

    done

    log "COMPLETED all entities imported into local mongo"
    log
fi

##############################################################################################################################
# Step 3: Export from mongo
##############################################################################################################################
if [[ ${operation} == "exportfrommongo" || ${operation} == "all" ]]; then

    # Prep directories
    rm -rf "${sql_file_path}"
    mkdir -p "${sql_file_path}"
    cd ${base_path}; cd ${mongo_scripts}

    log "STARTED creation of MySQL Load ready files to ${sql_file_path}"

    for coll in "${objs[@]}"
    do
        #TODO, make this support array expression with "'"
        addCols=`cat ${mapfile} | grep "${coll}" | cut -d'|' -f2 |tr "'" "|"  | sed 's/$/,/g' | tr -cd "[:print:]" | sed 's/,\+$//' `
        log "Exporting app.${coll} ... with $addCols"

        checkmongo;
        mongo ${tenant} --quiet --eval "var tenant='${tenant}';var coll='app.${coll}';var addCols='${addCols}'" ./exportCollection.js > "${sql_file_path}/${coll}.both.out"

        if [[ $? -ne 0 ]]; then log "Error in extracting data from Mongo for tenant ${tenant} for app.${coll}"; exit 1; fi

        grep -v RELATIONSHIPROWS "${sql_file_path}/${coll}.both.out" > "${sql_file_path}/${coll}.out"
        grep RELATIONSHIPROWS "${sql_file_path}/${coll}.both.out" | cut -d'|' -f2 >> "${sql_file_path}/RELATIONSHIPS.out"
        rm "${sql_file_path}/${coll}.both.out"
    done

    log "COMPLETED creation of MySQL Load ready files to ${sql_file_path}"
    log
fi

##############################################################################################################################
# Step 4: Import from files into MySQL
##############################################################################################################################
if [[ ${operation} == "importtosql" || ${operation} == "all" ]]; then

    log "STARTED import of files to MySQL ..."
    cd ${base_path}; cd ${sql_scripts}
    cmd="./importCollection.sh -s ${schema} -t ${tenant} -r drop -i ${sql_file_path}"
    if [[ ${columnfile} != "" ]]; then cmd="$cmd -c ${columnfile}"; fi
    now=`date '+%Y%m%d%H%M%S%N'`
    f="/tmp/${now}"

    echo "$cmd" > ${f}
    log "STARTING extraction $cmd"
    sh ${f}
    rm ${f}

    log "COMPLETED import of files to MySQL"
    log
fi

log "COMPLETED all processing with ${operation} operation"
