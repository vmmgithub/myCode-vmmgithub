#!/bin/bash

REMOVE_ONE_LINE_FILES="false"

if [[ "$1" == "--remove_one_line_files" ]]
then
	REMOVE_ONE_LINE_FILES="true"
	shift
fi

function remove_it() {
	local nI="$1"

	if [[ ! -s $nI ]]
	then
		echo "Removing empty file $nI"
		rm $nI
	elif [[ "$REMOVE_ONE_LINE_FILES" == "true" ]]
	then
		if [[ "$(wc -l $nI | cut -d ' ' -f 1)" == "1" ]]
		then
			echo "Removing one line file $nI"
			rm $nI
		fi
	fi
}

if [[ -z "$1" ]]
then
	for nI in *;
	do
		remove_it $nI
	done
else
	for nI in $@;
	do
		remove_it $nI
	done
fi
