#!/bin/bash

usage() { echo "Usage: $0 -t <tenant> -i <inputdir> [-d downloadfroms3] [-r droptables] [-c <columnfile>] -s [schema] " 1>&2; exit 1; }
log() { now=`date`; echo "[${now}] ${1} "; }

####################################################################################
# Get the command line arg
####################################################################################
while getopts ":t:c:i:d:r:s:" arg; do
    case "${arg}" in
        t) tenant=${OPTARG} ;;
        i) input=${OPTARG} ;;
        d) download=${OPTARG} ;;
        c) columnfile=${OPTARG} ;;
        r) droptables=${OPTARG} ;;
        s) schema=${OPTARG} ;;
        *) usage ;;
    esac
done
shift $((OPTIND-1))

if [[ -z "${tenant}" || -z "${input}" ]]; then
    usage
fi

if [[ -z "${schema}" ]]; then
  schema="${tenant}"
fi

####################################################################################
# Download of mongo extract from S3, when applicable
####################################################################################
if [[ "${download}" == "download" || "${download}" == "downloadfroms3" || "${download}" == "yes" ]]
then
  mkdir -p ${input}
  log "START download s3://Renew-Dev-Workspace/nithin/${tenant}/${tenant}.tar.gz file from S3 to the directory before importing ... "
  cd "${input}"
  s3cmd get "s3://Renew-Dev-Workspace/nithin/${tenant}/${tenant}.tar.gz" .
  tar xfz "${tenant}.tar.gz"
  rm "${tenant}.tar.gz"
  dir="${input}/${tenant}"
  log "COMPLETE download of file from s3"
fi

####################################################################################
# Global steps before proceeding
####################################################################################
now=`date '+%Y%m%d%H%M%S%N'`
f="/tmp/${now}.sql"

cat > ${f} <<EOF
  create database if not exists ${schema};
  use ${schema};

EOF

log "Initializing from map files ... "
. ../sh/readmap.sh '../sh/downloadAllOpps.default.map' "${columnfile}"
log "Completed Initializing"

####################################################################################
# Create of tables when required
####################################################################################
if [[ "${droptables}" == "droptables" || "${droptables}" == "drop" || "${droptables}" == "yes" ]]
then

  # Use helper script to read all the map  columns that are available for all tenants
  for coll in "${objs[@]}"; do
    
    case $coll in 
      opportunities) arr="${opportunities[@]}";;
      quotes) arr="${quotes[@]}";;
      bookings) arr="${bookings[@]}";;
      offers) arr="${offers[@]}";;
      assets) arr="${assets[@]}";;
      lineitems) arr="${lineitems[@]}";;
    esac

    tableName=`../js/sqlizeName.js -t "app.${coll}"`

    cmd=" drop TABLE if exists ${tableName}; \n"
    cmd="${cmd} CREATE TABLE ${tableName} ( \n"

    for var in ${arr}; do
      if [[ "${var}" != relationships* ]]; then
        s=`../js/sqlizeName.js -f "${var}"`
        n=`../js/sqlizeName.js -f "${var}"| cut -d' ' -f1`
        cmd="$cmd ${s} ,\n"
      fi
    done

    cmd="${cmd} PRIMARY KEY (_ID)\n ) ENGINE=InnoDB DEFAULT CHARSET=utf8; \n"

    echo -e ${cmd} >> ${f}
  done

  log "START drop of existing tables from ${schema} schema ... "

  # relationships table is not in the list, we should hand create it
  cat >> ${f} <<EOF
  create database if not exists ${schema};
  use ${schema};

  drop TABLE if exists RELATIONSHIPS;

  CREATE TABLE RELATIONSHIPS (
    SOURCETABLE varchar(250) DEFAULT NULL,
    SOURCEKEY varchar(250) DEFAULT NULL,
    DESTTABLE varchar(250) DEFAULT NULL,
    DESTKEY varchar(250) DEFAULT NULL,
    DESTNAME varchar(250) DEFAULT NULL,
    RELNAME varchar(250) DEFAULT NULL,
    PRIMARY KEY (SOURCEKEY,DESTKEY,RELNAME)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  create index REL_SRC on RELATIONSHIPS (SOURCEKEY); 
  create index REL_DST on RELATIONSHIPS (DESTKEY); 

  create index LIN_HD on APP_LINEITEMS (HEADERDOCUMENT_HEADERKEY); 

EOF

  mysql  < ${f}
  log "COMPLETE drop of existing tables from ${schema} schema  "

fi

####################################################################################
# Import of tables, by collection
####################################################################################
log "START import of data tables into ${schema} schema ... "

for coll in "${objs[@]}"; do
  tableName=`../js/sqlizeName.js -t "app.${coll}"`
  file="${input}/${coll}.out"

  log "   START import of ${tableName} ... "
  cat > ${f} <<EOF
    create database if not exists ${schema} default character set utf8;
    use ${schema};

    set unique_checks = 0;
    set foreign_key_checks = 0;
    set sql_log_bin = 0;
    set NAMES 'utf8';

    LOAD DATA LOCAL INFILE '${file}' REPLACE INTO TABLE ${tableName} fields terminated by ',' optionally enclosed by '"' ignore 1 lines;
EOF
  log "   COMPLETE import of ${tableName}"

  pre=`mysql -e "select count(1) from ${schema}.${tableName}"| tail -1`
  log "   pre count for ${tableName} is ${pre} "
  mysql  < ${f}
  post=`mysql  -e "select count(1) from ${schema}.${tableName}" | tail -1`
  log "   post count for ${tableName} is ${post} "

done

####################################################################################
# Relationships table needs to be handled separately
####################################################################################
log "   START import of RELATIONSHIPS ... "
pre=`mysql -e "select count(1) from ${schema}.RELATIONSHIPS"| tail -1`
log "   pre count for RELATIONSHIPS is ${pre} "

cat > ${f} <<EOF
create database if not exists ${schema};
use ${schema};

LOAD DATA LOCAL INFILE '${input}/RELATIONSHIPS.out' REPLACE INTO TABLE RELATIONSHIPS fields terminated by ',' optionally enclosed by '"';
EOF

mysql  < ${f}

post=`mysql  -e "select count(1) from ${schema}.RELATIONSHIPS" | tail -1`
log "   post count for RELATIONSHIPS is ${post} "
log "   COMPLETE import of ${tableName}"

####################################################################################
# Done
log "COMPLETE import of data tables into ${schema} schema  "
log "Done"
