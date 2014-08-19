#!/bin/bash

# this source line should not be required when this script is kicked off by
# the bookings_process.sh

#source ${CONFIG_FILE}

OPTIND=1

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

FTP_TARGET_DIR="$BASE_DIR/$CLIENT/${DT}_${CLIENT}"
ERROR_LOG="$FTP_TARGET_DIR/error.log"
ERROR_DAILY=
ERROR_WEEKLY=

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
# Daily files code
#
for ((nF=0;nF<${#DAILY_FILES[@]};nF++))
do
	echo "$(date) Getting file ${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT}"
	lftp << END_LFTP
		open sftp://$FTP_HOST
		user $FTP_USER $HP
		lcd $FTP_TARGET_DIR
		get $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT}
		exit 0
END_LFTP
	if [[ $? -ne 0 ]]
	then
		echo "$(date) ===ERROR downloading $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} ===" &>> $ERROR_LOG
		source ${LOG_SQL_SH} 'DownloadFromSFTP' 1 "$(date) ===ERROR downloading $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} ===" 0
		((ERROR_DAILY++))
	fi
	if [[ ! -s "$FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT}" ]]
	then
		echo "$(date) ===WARNING File  $FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} does not exist ===" &>> $ERROR_LOG
		source ${LOG_SQL_SH} 'DownloadFromSFTP' 1 "$(date) ===WARNING File  $FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} does not exist ===" 0
		rm $FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT}
		((ERROR_DAILY++))
		lftp << END_LFTP
			open sftp://$FTP_HOST
			user $FTP_USER $HP
			lcd $FTP_TARGET_DIR
			rm $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT}
			exit 0
END_LFTP
		if [[ $? -ne 0 ]]
		then
			echo "$(date) ===ERROR removing $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} ===" &>> $ERROR_LOG
			source ${LOG_SQL_SH} 'DownloadFromSFTP' 1 "$(date) ===ERROR removing $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} ===" 0
			((ERROR_DAILY++))
		fi
	else
		echo "$(date) ===SUCCESSFULLY Downloaded File $FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT}"
		source ${LOG_SQL_SH} 'DownloadFromSFTP' 1 "$(date) ===SUCCESSFULLY Downloaded File $FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT}" "`wc -l $FTP_TARGET_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} | awk '{print $1}'`"
		lftp << END_LFTP
			open sftp://$FTP_HOST
			user $FTP_USER $HP
			lcd $FTP_TARGET_DIR
			mv $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} $FTP_SRC_DIR/Archive/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT}
			exit 0
END_LFTP
		if [[ $? -ne 0 ]]
		then
			echo "$(date) ===ERROR archiving $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} ===" &>> $ERROR_LOG
			source ${LOG_SQL_SH} 'DownloadFromSFTP' 1 "$(date) ===ERROR archiving $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} ===" 0
			((ERROR_DAILY++))
		else
			echo "$(date) ===SUCCESSFULLY archived $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} ===" &>> $ERROR_LOG
			source ${LOG_SQL_SH} 'DownloadFromSFTP' 1 "$(date) ===SUCCESSFULLY archived $FTP_SRC_DIR/${DAILY_FILES[nF]}_${FILE_DT}.${FILE_EXT} ===" 0
		fi
	fi
done
#
# Weekly files code
#
if [[ "$WHAT_DAY" == "$WEEKLY_DAY" ]]
then
	for ((nF=0;nF<${#WEEKLY_FILES[@]};nF++))
	do
		echo "$(date) Getting file ${WEEKLY_FILES[nF]}_${FILE_DT}.${FILE_EXT}"
		lftp << END_LFTP
			open sftp://$FTP_HOST
			user $FTP_USER $HP
			lcd $FTP_TARGET_DIR
			get $FTP_SRC_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.${FILE_EXT}
			bye
END_LFTP
		if [[ $? -ne 0 ]]
		then
			echo "$(date) ===ERROR downloading $FTP_SRC_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.${FILE_EXT} ===" &>> $ERROR_LOG
			ERROR_WEEKLY=1
		fi
		if [[ ! -s "$FTP_TARGET_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.${FILE_EXT}" ]]
		then
			echo "$(date) ===ERROR File  $FTP_TARGET_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.${FILE_EXT} does not exist ===" &>> $ERROR_LOG
			ERROR_WEEKLY=1
			rm $FTP_TARGET_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.${FILE_EXT}
			lftp << END_LFTP
				open sftp://$FTP_HOST
				user $FTP_USER $HP
				lcd $FTP_TARGET_DIR
				rm $FTP_SRC_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.${FILE_EXT}
				bye
END_LFTP
		else
			lftp << END_LFTP
				open sftp://$FTP_HOST
				user $FTP_USER $HP
				lcd $FTP_TARGET_DIR
				mv $FTP_SRC_DIR/${WEEKLY_FILES[nF]}_${FILE_DT}.${FILE_EXT} $FTP_SRC_DIR/Archive/${WEEKLY_FILES[nF]}_${FILE_DT}.${FILE_EXT}
				bye
END_LFTP
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
	if [[ "$ERROR_DAILY" -ge "$ERROR_DAILY_COUNT" ]]
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
