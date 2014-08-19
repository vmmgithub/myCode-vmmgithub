#!/bin/bash

if [[ -z "$1" ]]
then
	echo "$(date) ERROR Empty extract directory name."
	echo "Usage: $0 <FTP Source directory with *.txt files>"
	exit 1
fi

if [[ ! -d "$1" ]]
then
	echo "$(date) ERROR $1 is not a directory."
	echo "Usage: $0 <FTP Source directory with *.txt files>"
	exit 1
fi

FTP_SOURCE_DIR="$1"

if [[ "`ls -1 $FTP_SOURCE_DIR/*.txt 2>/dev/null | wc -l`" == "0"  ]]
then
	echo "$(date) WARNING No *.txt files in $FTP_SOURCE_DIR directory"
	exit 0
fi

function CHECK_ERROR() {
	if [[ "$1" != "0" ]]
	then
		local ERR="$1"
		shift

		echo "ERROR $ERR during '$@'... Exiting function at $(date)"
		exit 1
	fi
}

FTP_HOST="74.201.119.36"
FTP_USER="smukerji@servicesource.com"
HP="x1xbjep"
FTP_TARGET_DIR="/Renew\ Data\ Services/Boomi\ Data/Tenant/Polycom/"

echo "$(date) ------------------------------------"
echo "$(date) Copying $FTP_SOURCE_DIR/*.txt to $FTP_HOST:$FTP_TARGET_DIR"
echo "$(date) ------------------------------------"

#
# Copy the files to destination.
#
lftp << END_LFTP
	open sftp://$FTP_HOST
	user $FTP_USER $HP
	lcd $FTP_SOURCE_DIR
	cd $FTP_TARGET_DIR
	mput $FTP_SOURCE_DIR/*.txt
	exit 0
END_LFTP
CHECK_ERROR $? "$(date) ===ERROR Copying $FTP_SOURCE_DIR/*.txt to $FTP_HOST:$FTP_TARGET_DIR"

echo "$(date) Copying *.txt to $FTP_HOST:$FTP_TARGET_DIR successfully completed."
