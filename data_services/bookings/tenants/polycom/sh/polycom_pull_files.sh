#!/bin/bash

BASE_DIR="/data/workspace/polycom/clientdata"
DT0=$1

if [[ -z $1 ]]
then
	DT0="$(date +%Y-%m-%d)"
fi

if [[ ! $DT0 =~ [2][0][1][3-9]-[0-1][0-9]-[0-3][0-9] ]]
then
	echo "$0 <import date in yyyy-mm-dd format>"
	echo "Example: $0 \"2014-05-21\""
	exit 1
fi

#
# Gets the day of the week
#
WHAT_DAY="$(date --date=$1 +%A)"
DT="$(date --date=$DT0 +%Y%m%d)"
#
# DATE to be used as the file suffix
#
FILE_DT="$(date --date=$DT0 +%Y_%m_%d)"

#1. DELIVERY_OF_DAILY_ORDERS_ACTIVITY_2013_10_06.txt
#2. ENTITLEMENT_DAILY_ACTIVITY_RENEW_2014_05_05.txt
#3. Weekly_Disassociated_Assets_2014_05_04.txt
#4. Weekly_Uninstall_Assets_2014_05_04.txt

ERROR_DAILY=
DAILY_FILES=(
"DELIVERY_OF_DAILY_ORDERS_ACTIVITY"
"ENTITLEMENT_DAILY_ACTIVITY_RENEW"
)
ERROR_WEEKLY=
WEEKLY_FILES=(
"Weekly_Uninstall_Assets"
"Weekly_Disassociated_Assets"
)

CLIENT="Polycom"
FTP_HOST="74.201.119.36"
FTP_USER="Polycomdatatrans"
HP="@0fPVGH2Mw"
FTP_SRC_DIR="/RenewOnDemand_Bookings_Reconciliation"

FTP_TARGET_DIR="$BASE_DIR/$CLIENT/${DT}_${CLIENT}"
ERROR_LOG="$FTP_TARGET_DIR/error.log"

#
# Empty the error.log if we are running the same second time.
#
rm -f $ERROR_LOG

echo "$(date) ------------------------------------"
echo "$(date) Step 1: Pulling files for $DT0 into $FTP_TARGET_DIR"
echo "$(date) ------------------------------------"
mkdir -p "$FTP_TARGET_DIR"

pushd $FTP_TARGET_DIR
FTP_COMMAND=
#
# Daily files are delivered at around 8am PST
#
for ((nF=0;nF<${#DAILY_FILES[@]};nF++))
do
	echo "$(date) Getting file ${DAILY_FILES[nF]}_${FILE_DT}.txt"
	lftp << END_LFTP
		open sftp://$FTP_HOST
		user $FTP_USER $HP
		lcd $FTP_TARGET_DIR
		get $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.txt
		exit 0
END_LFTP
	if [[ $? -ne 0 ]]
	then
		echo "$(date) ===ERROR downloading $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.txt ===" &>> $ERROR_LOG
		ERROR_DAILY=1
	fi
	if [[ ! -f "$FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.txt" ]]
	then
		echo "$(date) ===ERROR File  $FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.txt does not exist ===" &>> $ERROR_LOG
		ERROR_DAILY=1
	fi
done
#
# Weekly files are delivered on Sunday ~8am PST
#
if [[ "$WHAT_DAY" == "Sunday" ]]
then
	for ((nF=0;nF<${#WEEKLY_FILES[@]};nF++))
	do
		echo "$(date) Getting file ${WEEKLY_FILES[nF]}_${FILE_DT}.txt"
		lftp << END_LFTP
			open sftp://$FTP_HOST
			user $FTP_USER $HP
			lcd $FTP_TARGET_DIR
			get $FTP_SRC_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.txt
			bye
END_LFTP
		lftp -c "${FTP_COMMAND}"
		if [[ $? -ne 0 ]]
		then
			echo "$(date) ===ERROR downloading $FTP_SRC_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.txt ===" &>> $ERROR_LOG
			ERROR_WEEKLY=1
		fi
		if [[ ! -f "$FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.txt" ]]
		then
			echo "$(date) ===ERROR File  $FTP_TARGET_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.txt does not exist ===" &>> $ERROR_LOG
			ERROR_WEEKLY=1
		fi
	done
fi
popd

#
# Display error messages
#
if [[ -f $ERROR_LOG && -s $ERROR_LOG ]]
then
	echo "$(date) ===ERROR ERROR ERROR ERROR==="
	cat $ERROR_LOG
	echo "$(date) Check $ERROR_LOG file"
	echo "$(date) ===ERROR ERROR ERROR ERROR==="
	#
	# WEEKLY ERROR IS OK
	#
	if [[ "$ERROR_DAILY" == "1" ]]
	then
		exit 1
	fi
else
	#
	# remove the empty error log
	#
	rm -f $ERROR_LOG
	echo "$(date) Downloading successful.  No errors to report."
fi
