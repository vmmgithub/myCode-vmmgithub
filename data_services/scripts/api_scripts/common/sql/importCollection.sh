#!/bin/bash

usage() { echo "Usage: $0 -t <tenant> -i <inputdir> [-d downloadfroms3] [-r droptables] [-c <columnfile>] -s [schema] " 1>&2; exit 1; }
log() { now=`date`; echo "[${now}] ${1} "; }

importFile() {
  now=`date '+%Y%m%d%H%M%S%N'`
  fcs="/tmp/${now}"

  schema=${1};
  file=${2};
  tableName=${3};
  HEADERPRESENT=${4};

  if [[ ! -f ${file} ]]; then return; fi

  if [[ ${HEADERPRESENT} == "yes" ]]
  then
    IGNORE=" ignore 1 lines "
  fi

  cat > ${fcs} <<EOF
    create database if not exists ${schema} DEFAULT CHARSET=utf8;
    use ${schema};

    set unique_checks = 0;
    set foreign_key_checks = 0;
    set sql_log_bin = 0;
    set NAMES 'utf8';

    CREATE TABLE IF NOT EXISTS JOB_STATUSES (
      JOB varchar(250) DEFAULT NULL,
      TABLENAME varchar(250) DEFAULT NULL,
      STARTDATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      DESCRIPTION varchar(250) DEFAULT NULL,
      NUMBERRECORDS int(11) DEFAULT NULL,
      NUMBERERRORS int(11) DEFAULT NULL,
      STATUS varchar(250) DEFAULT NULL,
      MESSAGE text,
      ENDDATE timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
      UPDATEDATE timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

EOF

# Split the files into smaller pieces
# remove older files if they exist
if stat -t $file*PARTS* >/dev/null 2>&1
then
    rm $file*PARTS*
fi

../sh/splitFiles.sh -f ${file} -h no -l 50000 -p ${file}.PARTS

for FT in $file*PARTS*
do
  cat >> ${fcs} <<EOF
      LOAD DATA LOCAL INFILE '${FT}' REPLACE INTO TABLE ${tableName} fields terminated by ',' optionally enclosed by '"' ${IGNORE};
EOF
IGNORE="" # not needed after the first file
done

  pre=`mysql -e "select count(1) from ${schema}.${tableName}"| tail -1`
  log "   pre count for ${tableName} is ${pre} "
  mysql  < ${fcs}
  post=`mysql  -e "select count(1) from ${schema}.${tableName}" | tail -1`
  log "   post count for ${tableName} is ${post} "
}

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
# Check if current Tenant is already in Execution
####################################################################################

  if [[ -e skipFile ]]; then
	log "   skipFile is present, checking which Tenants to skip"
	if [[ `cat skipFile|grep $tenant` ]]; then
		log "   A previous instance of the script is already in execution for tenant $tenant, hence skipping execution"
		exit 0
	else
		echo "$tenant" >> skipFile
	fi
  else
	log "   skipFile is not present, hence creating one and continuing execution"
	echo "$tenant" > skipFile
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
log "Initializing from map files ... "
. ../sh/readmap.sh '../sh/downloadAllOpps.default.map' "${columnfile}"
log "Completed Initializing"

now=`date '+%Y%m%d%H%M%S%N'`
f="/tmp/${now}.ddl.sql"

cat > ${f} <<EOF
  create database if not exists ${schema} DEFAULT CHARSET=utf8;
  use ${schema};

EOF

####################################################################################
# Create of tables when required
####################################################################################
if [[ "${droptables}" == "droptables" || "${droptables}" == "drop" || "${droptables}" == "yes" ]]
then
  log "Recreating tables ... "

 # relationships table is not in the list, we should hand create it
  cat >> ${f} <<EOF
  drop TABLE if exists RELATIONSHIPS_TMP;

  CREATE TABLE RELATIONSHIPS_TMP (
    SOURCETABLE varchar(250) DEFAULT NULL,
    SOURCEKEY varchar(250) DEFAULT NULL,
    DESTTABLE varchar(250) DEFAULT NULL,
    DESTKEY varchar(250) DEFAULT NULL,
    DESTNAME varchar(250) DEFAULT NULL,
    RELNAME varchar(250) DEFAULT NULL,
    PRIMARY KEY (SOURCEKEY,DESTKEY,RELNAME)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  create index REL_SRC on RELATIONSHIPS_TMP (SOURCEKEY); 
  create index REL_DST on RELATIONSHIPS_TMP (DESTKEY); 

  alter table RELATIONSHIPS_TMP
    add INDEX ix_relation_dest(SOURCETABLE, DESTTABLE, RELNAME, DESTKEY),
    add INDEX ix_relation_src(SOURCETABLE, DESTTABLE, RELNAME, SOURCEKEY);

EOF

  # Use helper script to read all the map  columns that are available for all tenants
  for coll in "${objs[@]}"; do
    case $coll in 
      opportunities) arr="${opportunities[@]}";;
      quotes) arr="${quotes[@]}";;
      bookings) arr="${bookings[@]}";;
      offers) arr="${offers[@]}";;
      assets) arr="${assets[@]}";;
      lineitems) arr="${lineitems[@]}";;
      lookups) arr="${lookups[@]}";;
      products) arr="${products[@]}";;
      contacts) arr="${contacts[@]}";;
    esac

    tableName=`../js/sqlizeName.js -t "app.${coll}"`

    echo -e "\n Recreating Table: $tableName "
   
    cmd=" drop TABLE if exists ${tableName}_TMP; \n"
    cmd="${cmd} CREATE TABLE ${tableName}_TMP ( \n"

    for var in ${arr}; do
      if [[ "${var}" != relationships*keyNameType ]]; then
        s=`../js/sqlizeName.js -f "${var}"`
        cmd="$cmd ${s} ,\n"
      fi
    done

    cmd="${cmd} PRIMARY KEY (_ID)\n ) ENGINE=InnoDB DEFAULT CHARSET=utf8; \n"
    echo -e ${cmd} >> ${f}
  done

  log "START drop of existing tables from ${schema} schema ... "
  mysql  < ${f}
  log "COMPLETE drop of existing tables from ${schema} schema  "

fi

####################################################################################
# Import of tables, by collection
####################################################################################
log "START import of data tables into ${schema} schema ... "

for coll in "${objs[@]}"; do
  tableName=`../js/sqlizeName.js -t "app.${coll}"`

  log "   START import of ${schema}.${tableName}_TMP ... "
  importFile ${schema} ${input}/${coll}.out ${tableName}_TMP yes
  log "   COMPLETE import of ${schema}.${tableName}"

done

####################################################################################
# Switch TMP tables to Main tables, by collection
####################################################################################
log "   START switch of TMP tables to main tables depending on whether data is present ${schema} schema ... "

for coll in "${objs[@]}"; do
  tableName=`../js/sqlizeName.js -t "app.${coll}"`

  log "   START check whether ${schema}.${tableName}_TMP has data or not... "			

  echo "select count(*) from ${schema}.${tableName}_TMP" > temp
  if [[ `mysql -s < temp` > 0 ]]; then
	#actualtablename=`echo $tableName |sed 's/_TMP//g'`
	log "   ${schema}.${tableName}_TMP has data, hence dropping original table"
	echo "drop TABLE if exists ${schema}.${tableName};" > temp1
	echo "RENAME TABLE ${schema}.${tableName}_TMP TO ${schema}.${tableName};" >> temp1
	mysql -s < temp1
	
	if [[ $? != 0 ]]; then
		log "   Switch of table ${schema}.${tableName} was unsuccessful!! Please check"
	else
		log "   Switch of table ${schema}.${tableName} was successful!!"
	fi
  else
	log "   ${schema}.${tableName}_TMP does not have any data, hence retaining previous table"
  fi

done
####################################################################################
# Relationships table needs to be handled separately
####################################################################################
log "   START import of ${schema}.RELATIONSHIPS ... "
  importFile ${schema} ${input}/RELATIONSHIPS.out RELATIONSHIPS_TMP no
log "   COMPLETE import of ${schema}.RELATIONSHIPS"

####################################################################################
# Switching the TMP Relationships table
####################################################################################

  log "   START check whether ${schema}.RELATIONSHIPS_TMP has data or not... "

  echo "select count(*) from ${schema}.RELATIONSHIPS_TMP" > temp
  if [[ `mysql -s < temp` > 0 ]]; then

	log "   ${schema}.RELATIONSHIPS_TMP has data, hence dropping original table"
	echo "drop TABLE if exists ${schema}.RELATIONSHIPS;" > temp1
	echo "RENAME TABLE ${schema}.RELATIONSHIPS_TMP TO ${schema}.RELATIONSHIPS;" >> temp1
	mysql -s < temp1

	if [[ $? != 0 ]]; then
		log "   Switch of table ${schema}.RELATIONSHIPS was unsuccessful!! Please check"
	else
		log "   Switch of table ${schema}.RELATIONSHIPS was successful!!"
	fi
  else
	log "   ${schema}.RELATIONSHIPS_TMP does not have any data, hence retaining previous table"
  fi
####################################################################################

cat > ${f} <<EOF
  INSERT INTO JOB_STATUSES (JOB,DESCRIPTION, STATUS) VALUES ('DATA_REFRESH', 'Full data refresh from mongo export', 'COMPLETED');
EOF
mysql -s -f ${schema} < ${f}

####################################################################################

  log "   Removing temp files"
  rm temp temp1

  log "   Resetting the skipFile"
  grep -v $tenant skipFile > skipFile_tmp
  mv skipFile_tmp skipFile

####################################################################################
# Done
log "   COMPLETE import of data tables into ${schema} schema  "
log "   Done ${schema}"
