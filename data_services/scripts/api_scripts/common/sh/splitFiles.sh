#!/bin/bash

usage() { echo "Usage: $0 -f <FILENAME> [-l <LINECOUNT default 100000>] [-p PREFIX default FILENAME] [-h keepHeader default no] [-r rename no] [-v verbose] " 1>&2; exit 1; }
log() { now=`date`; echo "[${now}] ${1} "; }

# Get the command line arg
while getopts ":f:l:p:h:" arg; do
    case "${arg}" in
        f) FILENAME=${OPTARG} ;;
        l) LINECOUNT=${OPTARG} ;;
        p) PREFIX=${OPTARG} ;;
        h) KEEPHEADER=${OPTARG} ;;
        r) RENAME=${OPTARG} ;;
        v) verbose=${OPTARG} ;;
        *) usage ;;
    esac
done
shift $((OPTIND-1))

if [[ ! -f ${FILENAME} ]]
then
	echo "File ${FILENAME} does not exist"
	usage;
fi

if [[ -z $LINECOUNT ]]
then
	LINECOUNT=100000
fi

if [[ $PREFIX != "" ]]
then
	TPREFIX="${PREFIX}"
else
	TPREFIX=`echo ${FILENAME} | cut -d'.' -f1`
fi

SUFFIX=`echo ${FILENAME} | rev | cut -d'.' -f1 | rev`
header=`head -1 ${FILENAME}`
TMPNAME="${TPREFIX}_${RANDOM}."

split -l $LINECOUNT "${FILENAME}" "$TMPNAME"

if [[ ${RENAME} == "yes" ]]
then
	mv "${FILENAME}" "${FILENAME}.orig"
	if [[ ${verbose} == "yes" ]]; then echo "Renamed ${FILENAME} to ${FILENAME}.orig"; fi
fi

for F in $TMPNAME*
do
	N="$F.$SUFFIX"
	if [[ ${KEEPHEADER} == "yes" ]]; then echo ${header} > ${N}; fi
	cat $F >> "$N"
	rm $F
	if [[ ${verbose} == "yes" ]]; then echo "Created $N"; fi
done
