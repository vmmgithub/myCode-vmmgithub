#!/bin/bash
#
# Common file to be included in polycom_resolve*.sh files
#

PROCESS_DATE=
SCHEMA=
REPORT_ALL_EXCEPTIONS=

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
	echo "Usage: $0 -s <polycom|Polycom_UAT> -d <processing date in yyyy-mm-dd format>" 1>&2
	exit 1
}

####################################################################################
# Get the command line arg
####################################################################################
while getopts ":s:d:x" arg; do
	case "${arg}" in
		s) SCHEMA=${OPTARG} ;;
		d) PROCESS_DATE=${OPTARG} ;;
		x) REPORT_ALL_EXCEPTIONS="1" ;;
		*) usage ;;
	esac
done

shift $((OPTIND-1))

if [[ -z "${PROCESS_DATE}" || -z "${SCHEMA}" ]]
then
	usage
fi
if [[ ! "$PROCESS_DATE" =~ [2][0][0-9][0-9]-[0-1][0-9]-[0-3][0-9] ]]
then
	usage
fi

EX_DIR="/data/workspace/polycom/extracts/${PROCESS_DATE//-/}_PolycomExtracts"
mkdir -p "${EX_DIR}"
#
# Without 777 permission 'mysql' output write will fail
#
chmod 777 ${EX_DIR}

#
# Add Environment suffix
#
F_SUFFIX="${PROCESS_DATE//-/}_UAT"

if [[ "${SCHEMA}" == "polycom" ]]
then
	F_SUFFIX="${PROCESS_DATE//-/}_PROD"
fi
