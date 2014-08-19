#!/bin/bash

SS_ENV="$1"
shift

QT="$1"
SQL_HOME="/data/software/Implementations/data_services/bookings/tenants/polycom/sql"
SCRIPT_HOME="/data/software/Implementations/data_services/bookings/tenants/polycom/sh"
EX_HOME="/data/workspace/polycom/extracts"
#
# Valid quarters
#
QTA=(
"Q12013"
"Q22013"
"Q32013"
"Q42013"
"Q12014"
"Q22014"
)

function CHECK_ERROR() {
	if [[ "$1" != "0" ]]
	then
		echo "=====ERROR $QT == $2.. Exiting====="
		exit 1
	fi
}

function validate_quarter() {
	for ((nQ=0;nQ<${#QTA[@]};nQ++))
	do
		if [[ "${QTA[nQ]}" == "$1" ]]
		then
			return 0
		fi
	done
	echo "=====ERROR: Invalid Quarter $1.. Expecting ${QTA[*]} Exiting====="
	echo "Usage : $0 UAT|PROD Q[1-4]201[34]"
	echo "Example: $0 UAT Q12013"
	exit 1
}

SCHEMA=
if [[ $SS_ENV == "UAT" || "$SS_ENV" == "PROD" ]]
then
	if [[ "$SS_ENV" == "UAT" ]]
	then
		SCHEMA="Polycom_UAT"
	else
		SCHEMA="polycom"
	fi
else
	echo "Invalid environment $SS_ENV."
	echo "Usage : $0 UAT|PROD Q[1-4]201[34]"
	echo "Example: $0 UAT Q12013 Q22013"
	exit 1
fi
#
# Validate Quarter arguments
#
for NQ in $@
do
	validate_quarter $NQ
done

#
# Force creation of indexes
# Ignore the resulting errors...
#
#if [[ "$SS_ENV" == "PROD" ]]
#then
#	mysql -f < $SQL_HOME/polycom_indexes.sql
#else
#	mysql -f < $SQL_HOME/polycom_indexes_uat.sql
#fi

for QT in $@
do
	#
	# Validate the quarter to process
	#
	validate_quarter "$QT"

	DT=`date +%Y%m%d%H%M`
	#
	# Move *.csv files, if exists, to a directory so that they don't interfere with SQL script output files
	#
	if [[ "`ls -1 $EX_HOME/polycom_*.csv 2>/dev/null | wc -l`" != "0"  ]]
	then
		echo "$(date) === Moving existing *.csv files to $EX_HOME/moved_csv_files_${DT}"
		mkdir -p $EX_HOME/moved_csv_files_${DT}
		mv $EX_HOME/polycom_*.csv $EX_HOME/moved_csv_files_${DT}
		CHECK_ERROR "$?" "Moving *.csv files to $EX_HOME/moved_csv_files_${DT} directory"
	fi

	echo "$(date) === Copying $QT data to polycom_data.ENTITLEMENT_DAILY_ACTIVITY_HISTORICAL"
	mysql -e "
	truncate polycom_data.ENTITLEMENT_DAILY_ACTIVITY_HISTORICAL;
	set autocommit=0;
	insert into polycom_data.ENTITLEMENT_DAILY_ACTIVITY_HISTORICAL
	select * from polycom_data.ENTITLEMENT_DAILY_ACTIVITY_${QT};
	commit;"
	CHECK_ERROR $? "Copying $QT data to polycom_data.ENTITLEMENT_DAILY_ACTIVITY_HISTORICAL"

	echo "$(date) === Exceuting 2_polycom_historical_resolve_as_win.sql"
	if [[ "$SS_ENV" == "PROD" ]]
	then
		mysql -s < $SQL_HOME/2_polycom_historical_resolve_as_win.sql
	else
		mysql -s < $SQL_HOME/2_polycom_uat_historical_resolve_as_win.sql
	fi
	CHECK_ERROR $? "Running historical extracts"

	echo "$(date) === Moving .csv files to _${QT}_${DT}.csv suffix"
	for nI in $EX_HOME/*.csv;
	do
		mv $nI ${nI//.csv}_${QT}_${DT}_${SS_ENV}.csv
	done

	echo "$(date) === Moving .csv files to $EX_HOME/${QT}_${DT} directory"
	mkdir -p $EX_HOME/${QT}_${DT}
	mv $EX_HOME/*.csv $EX_HOME/${QT}_${DT}
	#
	# Remove empty files
	#
	$SCRIPT_HOME/remove_empty_files.sh --remove_one_line_files $EX_HOME/${QT}_${DT}/*.csv
done
echo "$(date) === Processing complete"
