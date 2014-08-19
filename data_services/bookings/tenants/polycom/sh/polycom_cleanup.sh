#!/bin/bash
#
# Cleanup a failed day's data so that it can be re-run.
#
INC_DATE=
SS_ENV0=
SCHEMA=
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
	echo "Usage: $0 -e <PROD|UAT> -d <yyyy-mm-dd>" 1>&2
	exit 1
}

while getopts ":e:d:s:x" arg; do
	case "${arg}" in
		e) SS_ENV0=${OPTARG} ;;
		d) INC_DATE=${OPTARG} ;;
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
	echo "ERROR Invalid date $INC_DATE"
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
	echo "ERROR Invalid environment $SS_ENV0."
	usage
fi

DATE_SUFFIX="${INC_DATE//-/}"

#
# 1. Move the downloaded files to archive, if exists
# 2. Rename the old extracts directory, if exists
# 3. Delete the rows from polycom_data tables, if exists
#
DOWNLOAD_DIR="/data/workspace/polycom/clientdata/Polycom/${DATE_SUFFIX}_Polycom"
ARCHIVE_DIR="${DOWNLOAD_DIR}/archive"

echo "$(date) Moving files from ${DOWNLOAD_DIR}/*.* ${ARCHIVE_DIR}"
mkdir -p ${ARCHIVE_DIR}
mv -f ${DOWNLOAD_DIR}/*_${INC_DATE//-/_}.txt ${ARCHIVE_DIR}

EXTRACT_DIR="/data/workspace/polycom/extracts/${DATE_SUFFIX}_PolycomExtracts"
ARCHIVE_DIR="${EXTRACT_DIR}/archive"

echo "$(date) Moving files from ${EXTRACT_DIR}/*${SS_ENV}.* ${ARCHIVE_DIR}"
mkdir -p ${ARCHIVE_DIR}
mv -f ${EXTRACT_DIR}/*${SS_ENV}.* ${ARCHIVE_DIR}

echo "$(date) Deleting records from tables polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY, polycom_data.ENTITLEMENT_DAILY_ACTIVITY, polycom_data.WEEKLY_DISASSOCIATED_ASSETS, polycom_data.WEEKLY_UNINSTALL_ASSETS, and polycom_data.REACTIVATED_AGREE_PO with SS_IMPORT_DT='${INC_DATE}'"
mysql -s -s polycom_data -e "
delete from polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY where SS_IMPORT_DT='${INC_DATE}';
delete from polycom_data.ENTITLEMENT_DAILY_ACTIVITY where SS_IMPORT_DT='${INC_DATE}';
delete from polycom_data.WEEKLY_DISASSOCIATED_ASSETS where SS_IMPORT_DT='${INC_DATE}';
delete from polycom_data.WEEKLY_UNINSTALL_ASSETS where SS_IMPORT_DT='${INC_DATE}';
delete from polycom_data.REACTIVATED_AGREE_PO where SS_IMPORT_DT='${INC_DATE}';
"

echo "$(date) Processing complete."
