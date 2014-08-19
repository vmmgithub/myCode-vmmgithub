#!/bin/bash

#
# Overarching bookings reconciliation process to be run daily
#

export INC_DATE=
export SS_ENV0=
export SQL_INDEX="1"
export SCHEMA=

function usage() {
	echo "Usage: $0 -t <tenant> -c <config file> -q <schema> -e <PROD|UAT> -d <yyyy-mm-dd> [-s] (to skip SQL index creation)" 1>&2
	exit 1
}

while getopts ":e:d:s:t:q:c:" arg; do
	case "${arg}" in
		e) export SS_ENV0=${OPTARG} ;;
		d) export INC_DATE=${OPTARG} ;;
		s) export SQL_INDEX="0" ;;
		t) export TENANT=${OPTARG} ;;
		q) export SCHEMA=${OPTARG} ;;
		c) export CONFIG_FILE=${OPTARG} ;;
		*) usage ;;
	esac
done
shift $((OPTIND-1))

#
# Convert environment argument to upper case
#
export SS_ENV="$(echo $SS_ENV0 | tr [a-z] [A-Z])"

#
# Check for Date
#
if [[ ! $INC_DATE =~ [2][0][0-9][0-9]-[0-1][0-9]-[0-3][0-9] ]]
then
	echo "Invalid Date $1"
	echo "$0 <date in yyyy-mm-dd format> UAT|PROD"
	echo "Example: $0 \"2014-05-21\" UAT"
	exit 1
fi

export DATE_SUFFIX="${INC_DATE//-/}"
export LOAD_DT_SUFFIX="${INC_DATE//-/_}"

# source config after getting the date
source ${CONFIG_FILE}
mkdir -p ${TMP_DIR}

function CHECK_ERROR() {
	if [[ "$1" != "0" ]]
	then
		local ERR="$1"
		shift

		echo "ERROR $ERR during '$@'... Exiting function at $(date)"
		exit 1
	fi
}
#
# Process
# 0. Check that the Renew daily refresh has been loaded into mysql03 for tenant
# 1. Move already processed files to 'processed' or archive dir
# 2. Download files from SFTP server
# 3. Load data into the MySQL server
# 4. Run stored procedure
# 5. Remove empty files
# 6. Execute the scrub files using the ".js" scripts in sequence.
#

# Until the daily refresh from Renew has completed, do not attempt bookings
# reconciliation.

while [[ "`grep \"Completed refreshing MySQL data for tenant ${TENANT}\" ${DAILY_REFRESH_LOG_FILE} 2> /dev/null | wc -l`" == "0" ]]
do
	echo "$(date) === Daily Refresh for ${TENANT} has not completed ==="
	sleep 5m
done


#
# Move previous day's files to processed directory
#

mkdir -p $DAT_FILE_DIR/processed

if [[ "`ls -1 $DAT_FILE_DIR/*.${FILE_EXT} 2>/dev/null | wc -l`" != "0"  ]]
then
	mv $DAT_FILE_DIR/*.${FILE_EXT} $DAT_FILE_DIR/processed
	CHECK_ERROR "$?" "Moving files to $DAT_FILE_DIR/processed directory"
fi

echo "$(date) === Pulling ${TENANT} client files from FTP directory with \"$INC_DATE\" argument"
source $SCRIPT_HOME/pull_files.sh $INC_DATE
CHECK_ERROR "$?" "Downloading files"

echo "$(date) === Loading downloaded files from $DAT_FILE_DIR with ${DATE_SUFFIX} suffix into Mysql"
source $SCRIPT_HOME/${TENANT}_bookings_load.sh -i "$INC_DATE" $DAT_FILE_DIR/*${LOAD_DT_SUFFIX}*.${FILE_EXT}
CHECK_ERROR "$?" "Loading data into MySQL"

#
# Copy sql file scripts for future archiving
#
cp $SQL_HOME/*.sql $SQL_TMP_DIR
CHECK_ERROR "$?" "Copying files to $SQL_TMP_DIR"

#
# This assumes mysql is setup with default login
# Ignore errors from index creation
#
if [[ "$SQL_INDEX" == "1" ]]
then
	echo "$(date) === Running \"$SQL_TMP_DIR/${TENANT}_indexes.sql\""
	echo "IGNORE the following index errors..."
	mysql -u $USERID -h$MYSQLHOST $SCHEMA -s -f < $SQL_TMP_DIR/${TENANT}_indexes.sql
fi

#
# Move *.csv files, if they exist, to a directory so that they don't interfere with SQL script output files
#
if [[ "`ls -1 $EX_HOME/*.csv 2>/dev/null | wc -l`" != "0"  ]]
then
	DT=`date +%Y%m%d%H%M`
	echo "$(date) === Moving existing *.csv files to $EX_HOME/moved_csv_files_${DT}"
	mkdir -p $EX_HOME/moved_csv_files_${DT}
	mv $EX_HOME/*.csv $EX_HOME/moved_csv_files_${DT}
	CHECK_ERROR "$?" "Moving *.csv files to $EX_HOME/moved_csv_files_${DT} directory"
fi

# Check for that there were client files today. Run the SQL scripts for bookings
# reconciliation logic and for generating the extracts.

if [[ "`ls -1 ${DAT_FILE_DIR}/*.csv 2>/dev/null | wc -l`" != "0" ]]
then
	echo "$(date) === Running \"$SQL_TMP_DIR/${TENANT_STORED_PROC}\""
	mysql -u $USERID -h$MYSQLHOST $SCHEMA -s < $SQL_TMP_DIR/${TENANT_STORED_PROC}
	CHECK_ERROR "$?" "Running ${TENANT} Stored Procedure"
	echo "$(date) === Running \"$SQL_TMP_DIR/${TENANT_EXTRACT_PROC}\""
	source ${SCRIPT_HOME}/${TENANT_CREATE_SCRUB_SH}
	CHECK_ERROR "$?" "Running ${TENANT} Extract Stored Procedure"
fi

#
# Remove temporary SQL files
#
#rm -f $SQL_TMP_DIR/*.sql
#CHECK_ERROR "$?" "Removing temporary SQL files from $SQL_TMP_DIR"

#echo "$(date) === Removing empty scrub files"
#$SCRIPT_HOME/remove_empty_files.sh --remove_one_line_files $EX_HOME/*.csv $EX_HOME/*.txt
#CHECK_ERROR "$?" "Removing emptry scrub files"

# Create the extract directory for the final version of the extract files

X_DIR="$EX_HOME/${DATE_SUFFIX}_${TENANT}_Extracts"
mkdir -p $X_DIR
sleep 1
#
# Add the date suffix to the extract files
#
echo "$(date) === Moving scrub files with ${X_DIR}"
if [[ "`ls -1 $TMP_EX_HOME/*.csv 2>/dev/null | wc -l`" != "0"  ]]
then
	for F in $TMP_EX_HOME/*.csv
	do
		# check if the input file only has the header row, if it does delete
		# the csv and sh file and move to the next set
		if [[ "`wc -l ${F} | awk '{print $1}'`" == "1" ]]
		then
			rm ${F} ${F%.csv}.sh
			echo "$(date) === ${F} was only the header record - no other data"
			source ${LOG_SQL_SH} "${F} record count" 1 "${F} did not have any updates" 1
			continue
		fi
		BN="$(basename $F)"
		N="${BN%.csv}"
		N="${N}_${SS_ENV}.csv"
		mv ${F} $X_DIR/${N}
		F2="${F%.csv}.sh"
		BN2="$(basename $F2)"
		N2="${BN2%.sh}"
		N2="${N2}_${SS_ENV}.sh"
		mv ${F2} $X_DIR/${N2}
		LOG="${X_DIR}/${N2%.sh}.log"
		# this adds to the shell script to switch to the sjs directory before
		# executing the js command
		echo -e "#!/bin/bash\ncd /data/software/Implementations/data_services/scripts/api_scripts/common/js\n\n$(cat $X_DIR/${N2})" > $X_DIR/${N2}
		# this sed command appends the --file argument to the js command in the
		# shell script
		sed -i '4s#$# --file '"$X_DIR/${N} > $LOG"'#' $X_DIR/${N2}
		CHECK_ERROR "$?" "Moving scrub files to ${X_DIR} and updating scrub file commands"
	done
fi
sleep 1

#
# Add the date prefix to the data load files
#
if [[ "`ls -1 $EX_HOME/*.txt 2>/dev/null | wc -l`" != "0"  ]]
then
	for F in $EX_HOME/*.txt
	do
		BN="$(basename $F)"
		N="${BN%.txt}"
		N="${DATE_SUFFIX}_${N}_${SS_ENV}.csv"
		mv ${F} $X_DIR/${N}
		CHECK_ERROR "$?" "Renaming data load files with $DATE_SUFFIX prefix"
	done
fi

#
# Update the scrubs to Renew
#
echo "$(date) === Updating Renew with scrub files"

if [[ "`ls -1 $X_DIR/*.sh 2>/dev/null | wc -l`" != "0" ]]
then
	for ((nF=0;nF<${#SCRUB_FILES[@]};nF++))
	do
		F=$X_DIR/${SCRUB_FILES[nF]}
		echo "$(date) === Running ${F}"
		source ${F}
		CHECK_ERROR "$?" "Running scrub command ${F}"
		if [[ "`grep '\[error\]' ${F%.sh}.log 2> /dev/null | wc -l`" != "0" ]]
		then
			echo "ERROR During Scrub Execution of ${F}"
			source ${LOG_SQL_SH} "Running scrub command ${F}" 1 "ERROR During Scrub Execution of ${F}" "`grep -i error ${F%.sh}.log 2> /dev/null | wc -l | awk '{print $1}'`"
			exit 1
		fi
		echo "$(date) === Successfully ran ${F}"
		source ${LOG_SQL_SH} "Running scrub command ${F}" 1 "Successfully Ran Scrub Execution of ${F}" "`wc -l ${F%.sh}.log 2> /dev/null | awk '{print $1}'`"
	done
fi

CHECK_ERROR "$?" "Updating Renew with scrub files"

mysql -u $USERID -h$MYSQLHOST $SCHEMA -s < $SQL_TMP_DIR/${TENANT_COMPLETE_PROC}

echo "$(date) === Processing complete for \"$INC_DATE\" argument"
