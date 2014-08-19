#!/bin/bash

#
# Overarching Polycom process to be run daily
#

INC_DATE=
SS_ENV0=
SQL_INDEX="0"
SCHEMA=
REPORT_ALL_EXCEPTIONS=""

function CHECK_ERROR() {
	if [[ "$1" != "0" ]]
	then
		local ERR="$1"
		shift

		echo "ERROR $ERR during '$@'... Exiting function at $(date)"
		exit 1
	fi
}

function usage() {
	echo "Usage: $0 -e <PROD|UAT> -d <yyyy-mm-dd> [-q] (to force SQL index creation) [-x] (to report exceptions for all data in polycom_data tables)" 1>&2
	exit 1
}

while getopts ":e:d:s:x" arg; do
	case "${arg}" in
		e) SS_ENV0=${OPTARG} ;;
		d) INC_DATE=${OPTARG} ;;
		q) SQL_INDEX="1" ;;
		x) REPORT_ALL_EXCEPTIONS="-x" ;;
		*) usage ;;
	esac
done
shift $((OPTIND-1))

#
# Convert environment argument to upper case
#
SS_ENV="$(echo $SS_ENV0 | tr [a-z] [A-Z])"

#
# Check for Date
#
if [[ ! "$INC_DATE" =~ [2][0][0-9][0-9]-[0-1][0-9]-[0-3][0-9] ]]
then
	echo "Invalid Date $INC_DATE"
	usage
fi
#
# Check for environment
#
if [[ $SS_ENV == "UAT" || "$SS_ENV" == "PROD" ]]
then
	if [[ "$SS_ENV" == "UAT" ]]
	then
		SCHEMA="Polycom_UAT"
	else
		SCHEMA="polycom"
	fi
else
	echo "Invalid environment $SS_ENV0."
	usage
fi

DATE_SUFFIX="${INC_DATE//-/}"
LOAD_DT_SUFFIX="${INC_DATE//-/_}"

#
# Process
# 0. Move already processed files to 'processed' or archive dir
# 1. Download files from FTP server
# 2. Load data into the MySQL server
# 3. Update base tables in MySQL Server
# 4. Run 'Close as Loss' script
# 5. Run 'Close as Win' script
# 6. Remove empty files
# 7. Execute the scrub files using the ".js" scripts in sequence.
#

#
# DEFINE vars
#
EX_HOME="/data/workspace/polycom/extracts"
SCRIPT_HOME="/data/software/Implementations/data_services/bookings/tenants/polycom/sh"
INP_DIR="/data/software/Implementations/data_services/bookings/tenants/polycom/data/boomi_load"
SJS_HOME="/data/software/Implementations/data_services/scripts/api_scripts/common/js"
DAT_FILE_DIR="/data/workspace/polycom/clientdata/Polycom/${DATE_SUFFIX}_Polycom"
LINKER_DIR="/data/software/Implementations/data_services/scripts/api_scripts/linker/scripts"
SQL_HOME="/data/software/Implementations/data_services/bookings/tenants/polycom/sql"
X_DIR="$EX_HOME/${DATE_SUFFIX}_PolycomExtracts"

#
# Move previous day's txt files to processed directory
#
mkdir -p $DAT_FILE_DIR/processed

if [[ "`ls -1 $DAT_FILE_DIR/*.txt 2>/dev/null | wc -l`" != "0"  ]]
then
	mv $DAT_FILE_DIR/*.txt $DAT_FILE_DIR/processed
	CHECK_ERROR "$?" "Moving files to $DAT_FILE_DIR/processed directory"
fi

echo "$(date) === Pulling Polycom client files from FTP directory with \"$INC_DATE\" argument"
$SCRIPT_HOME/polycom_pull_files.sh $INC_DATE
CHECK_ERROR "$?" "Downloading files"

echo "$(date) === Loading downloaded files from $DAT_FILE_DIR with ${DATE_SUFFIX} suffix into Mysql"
$SCRIPT_HOME/polycom_load.sh -i "$INC_DATE" $DAT_FILE_DIR/*${LOAD_DT_SUFFIX}*.txt
CHECK_ERROR "$?" "Loading data into MySQL"

#
# This assumes mysql is setup with default login
# Ignore errors from index creation
#
if [[ "$SQL_INDEX" == "1" ]]
then
	echo "$(date) === Running \"$SQL_HOME/polycom_indexes.sql.sh\""
	echo "IGNORE the following index errors..."
	$SQL_HOME/polycom_indexes.sql.sh -s $SCHEMA -f
fi

#
# Move *.csv files, if exists, to a directory so that they don't interfere with
#	SQL script output files
#
SALT="${RANDOM}"
if [[ "`ls -1 $X_DIR/polycom_*.csv 2>/dev/null | wc -l`" != "0"  ]]
then
	echo "$(date) === Moving existing polycom_*.csv files to $X_DIR/archive"
	mkdir -p $X_DIR/archive
	for F in $X_DIR/polycom_*.csv
	do
		BN="$(basename $F)"
		N="${BN%.csv}"
		N="${N}_${SALT}.csv"
		mv ${F} $X_DIR/archive/${N}
		CHECK_ERROR "$?" "Command mv ${F} $X_DIR/archive/${N}"
	done
fi
if [[ "`ls -1 $X_DIR/Polycom*.csv 2>/dev/null | wc -l`" != "0"  ]]
then
	echo "$(date) === Moving existing Polycom_*.csv files to $X_DIR/archive"
	mkdir -p $X_DIR/archive
	for F in $X_DIR/Polycom_*.csv
	do
		BN="$(basename $F)"
		N="${BN%.csv}"
		N="${N}_${SALT}.csv"
		mv ${F} $X_DIR/archive/${N}
		CHECK_ERROR "$?" "Command mv ${F} $X_DIR/archive/${N}"
	done
fi
#
# Run this only if UNINSTALL & DISASSOCIATE files are downloaded or if it is Sunday
#
WHAT_DAY="$(date --date=$INC_DATE +%A)"
if [[ "`ls -1 $DAT_FILE_DIR/*Uninstall* 2>/dev/null | wc -l`" != "0" || "`ls -1 $DAT_FILE_DIR/*Disassociate* 2>/dev/null | wc -l`" != "0" || "$WHAT_DAY" == "Sunday" ]]
then
	echo "$(date) === Running \"$SQL_HOME/polycom_resolve_as_loss.sql.sh\""
	$SQL_HOME/polycom_resolve_as_loss.sql.sh -s $SCHEMA -d $INC_DATE $REPORT_ALL_EXCEPTIONS
	CHECK_ERROR "$?" "Running Resolve As Loss"
fi

#
# Run the Resolve as win script
#
echo "$(date) === Running \"$SQL_HOME/polycom_resolve_as_win.sql.sh\""
$SQL_HOME/polycom_resolve_as_win.sql.sh -s $SCHEMA -d $INC_DATE $REPORT_ALL_EXCEPTIONS
CHECK_ERROR "$?" "Running Resolve As Win"

#
# Convert the 777 permission to 700
#
if [[ -d "$X_DIR" ]]
then
	chmod 700 "$X_DIR"
fi

echo "$(date) === Removing empty scrub files"
$SCRIPT_HOME/remove_empty_files.sh --remove_one_line_files $X_DIR/polycom_*.csv $X_DIR/Polycom_*.csv
CHECK_ERROR "$?" "Removing empty scrub files"

#
# Transfer the Polycom_ReactivationFee_SERVICE_ASSET_DATE.csv file to the destination
#
if [[ "`ls -1 $X_DIR/Polycom_Reactivation*.csv 2>/dev/null | wc -l`" != "0"  ]]
then
	$SCRIPT_HOME/polycom_put_files.sh $X_DIR
	if [[ "$?" != "0" ]]
	then
		echo "$(date) === ERROR during copying Polycom_Reactivation*.csv files FTP server.  CONTINUING..."
	fi
else
	echo "$(date) === No Polycom_ReactivationFee_SERVICE_ASSET_*.csv files to transfer."
fi

#
# Update the scrubs to Renew
#
echo "$(date) === Updating Renew with scrub files"
$SCRIPT_HOME/polycom_update_renew.sh -d "$INC_DATE" -s $SS_ENV
CHECK_ERROR "$?" "Updating Renew with scrub files"

#
# Run Stored Procs to create Boomi load ready files for different entities and send files to SFTP server
#
cd $SCRIPT_HOME
. ./executeStoredProcLFTP_Dynamic.sh $INC_DATE

if [[ $? != 0 ]]; then
        echo -e " The Boomi Load ready files did not get generated correctly or there was SFTP issue"
else
        echo -e " The Boomi Load ready files have been generated successfully and SFTP'd to the server"
fi

#
# Setting up ENV Variable
#
echo "SS_ENV is $SS_ENV"
if [[ $SS_ENV = 'UAT' ]]; then
        lnk_env=uat02dl-int.ssi-cloud.com
else
        lnk_env=prod02dl-int.ssi-cloud.com
fi

#
# Allowing Buffer Time for the Load Batches to be picked up by Boomi and load into Renew
#
echo -e "\n  Allowing Buffer Time for the Load Batches to be picked up by Boomi and load into Renew"
sleep 2700


#
# Run linker for all the entities thus loaded into Renew
#
cd $LINKER_DIR
echo -e "\nExecuting Linker Process"
./linker.js --operation cruiseControl --tenant polycom --limit 2 --host $lnk_env

while [[ 1 ]]; do
        echo -e "\n Checking to see if linker process has completed or not"
        if [[ `./scripts/linker.js --operation countJobs --host $lnk_env --tenant polycom|grep 'No records found'` ]]; then
                echo -e "\n All linker jobs have completed!! Exiting Loop"
                break
        else
                echo -e "\n All linker jobs have not completed!! Continuing in Link Loop"
                sleep 300
        fi
done

#
#Creating Batch Quarter File for Op-Gen
#
  echo -e " Creating Batch Quarter File for Op-Gen \n"
  INT_DATE_NEW="'"$INC_DATE"'"

  while IFS=',' read schema tableName; do

        echo "use $schema;" > $SCRIPT_HOME/file_dyn
        echo "call SP_Dt_Sell_Prd_Dynamic("${tableName}","${INT_DATE_NEW}");" >> $SCRIPT_HOME/file_dyn
        mysql -s < $SCRIPT_HOME/file_dyn

  done<$SCRIPT_HOME/Op_Gen_dyn
  rm  $SCRIPT_HOME/file_dyn


#
#Creating Op-Gen Detailed Input file
#
  echo -e "Creating Op-Gen Detailed Input file"
  while IFS=',' read Start_Dt End_Dt Sell_Prd; do

	while read theatre; do
  echo "polycom|uat02dl-int.ssi-cloud.com|443|data.admin@polycom.com|pass@word123|{\"type\":\"app.asset/service\",\"endDate\":{\"\$gte\":\"$Start_Dt\",\"\$lte\":\"$End_Dt\"},\"associatedOpportunity\":\"false\",\"extensions.master.clientTheatre.value.name\":\"$theatre\"}|extensions.master.batchType.value.name:renewal,extensions.master.businessLine.value.name:core,extensions.master.commitLevel.value.name:black,extensions.master.targetPeriod.value.name:$Sell_Prd">>$INP_DIR/Polycom_Final_Op_Gen_Inp_File.txt 	
  done<$SCRIPT_HOME/Theatre_File

  done<$INP_DIR/Date_Sell_Prd_Dtls.txt

#
#Run Op-Gen
#

  while IFS='|' read tenant host port user password filter criteria
  do
  echo -e "\n\n Starting Op-Gen for the following !!! \n\n"
  echo -e " tenant=$tenant"
  echo -e " host=$host"
  echo -e " port=$port"
  echo -e " user=$user"
  echo -e " password=$password"
  echo -e " filter=$filter"
  criteria="'"$criteria"'"
  echo -e " criteria=$criteria"

  export theatre=`echo $filter|awk -F "extensions.master.clientTheatre.value.name\":\"" '{ print $2 }'|awk -F"\"" '{ print $1 }'|sed 's/}//g'`
  export batch_quarter=`echo $criteria | awk -F "extensions.master.targetPeriod.value.name:" '{ print $2 }'|awk -F "," '{ print $1 }'`
  echo " Theatre=$theatre"
  echo -e " Batch_Quarter=$batch_quarter \n"

  cd $SJS_HOME
  ./generateOpportunities.js --tenant $tenant --host $host --port $port --user $user --password $password --filter $filter --criteria $criteria --operation generateAndPoll

  if [[ $? -ne 0 ]]; then
  	echo -e "\n\n Op-Gen did not complete successfully for the above conditions"
  fi

  echo " Going to Sleep to allow the server to cool down !!"
  sleep 120

  done<$INP_DIR/Polycom_Final_Op_Gen_Inp_File.txt

  echo "$(date) === Processing complete for \"$INC_DATE\" argument"
