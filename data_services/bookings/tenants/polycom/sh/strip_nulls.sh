#!/bin/bash

#
# To remove '\N' --> NULL characters.
# Strips '\N' characters with \t as a prefix and or suffix from the file
#
CUR_TIME=`date +%Y%m%d%H%M%S`
function compress_backup() {
	for fle in $@; do
		gzip $fle.$CUR_TIME
	done
}

if [ -n "$1" ]
then
	#
	# Make a backup of original with timestamp
	#
	for original in $@; do
		cp $original $original.$CUR_TIME
	done
	#
	# Replace the middle '\N's (Need to do this twice because of the way sed acts)
	# Replace the end '\N's
	# Replace the beginning '\N's
	#
	for src in $@; do
		LANG=UTF-8 sed -i -e 's/\t\\N\t/\t\t/g' \
			-e 's/\t\\N\t/\t\t/g' \
			-e 's/\t\\N$/\t/g' \
			-e 's/^\\N\t/\t/g' $src
	done
	compress_backup $@&
else
	echo "Usage '$0' <fileName>"
fi
