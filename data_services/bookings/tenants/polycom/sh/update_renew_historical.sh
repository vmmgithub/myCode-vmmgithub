#!/bin/bash

function CHECK_ERROR() { if [[ "$1" != "0" ]]; then echo "=====ERROR $1 == $2.. Exiting====="; exit 1; fi }
function usage() { echo "Usage: $0 -q <Q22013> -e <PROD|UAT> -t <TENANT> -p <filecreateprefix> -h <envt-url> -m <ALL|PREP|SPLIT|CLOSE>" 1>&2; exit 1; }
function log() { now=`date`; echo "[${now}] ${1} "; }

####################################################################################
# Get the command line arg
####################################################################################
while getopts ":q:t:e:p:h:m:" arg; do
 case "${arg}" in
  t) TENANT=${OPTARG} ;;
  q) QTR=${OPTARG} ;;
  e) ENVT=${OPTARG} ;;
  p) PREF=${OPTARG} ;;
  h) URL=${OPTARG} ;;
  m) MODE=${OPTARG} ;;
  *) usage ;;
 esac
done
shift $((OPTIND-1))

if [[ -z "${QTR}" || -z "${TENANT}" || -z "${ENVT}" || -z "${URL}" || -z "${MODE}" ]]; then
 usage
fi

# Changing directory to sh 
cd "/data/software/Implementations/data_services/scripts/api_scripts/common/sh"

# Copy the extracts from extracts folder to loading folder.
#cp /data/workspace/polycom/extracts/polycom*_${QTR}.csv /data/software/Implementations/data_services/data/input

QTRX="${QTR}_${PREF}_${ENVT}"
INPUT_DIR="/data/workspace/${TENANT}/extracts/${QTR}_${PREF}"
LOG_DIR="/data/software/Implementations/data_services/data/prd/logs"
JS="/data/software/Implementations/data_services/scripts/api_scripts/common/js"

if [[ "${MODE}" == "ALL" || "${MODE}" == "PREP" ]]
then
	echo "===$(date) Step 1==="
    ./executeTillCompletion.sh -t ${TENANT} -h $URL -o multiAttributes -f $INPUT_DIR/${TENANT}_entitlement_update_offers_with_values_in_entitlement_${QTRX}.csv -a "-s app.offer -o update" &> $LOG_DIR/${TENANT}_entitlement_update_offers_with_values_in_entitlement_${QTRX}.log
	CHECK_ERROR $? "Step 1"

	echo "===$(date) Step 2==="
    ./executeTillCompletion.sh -t ${TENANT} -h $URL -o multiAttributes -f $INPUT_DIR/${TENANT}_entitlement_calculate_split_opportunity_with_unmatched_offers_${QTRX}.csv -a "-s app.opportunity -o update" &> $LOG_DIR/${TENANT}_entitlement_calculate_split_opportunity_with_unmatched_offers_${QTRX}.log
	CHECK_ERROR $? "Step 2"

	echo "===$(date) Step 3==="
    ./executeTillCompletion.sh -t ${TENANT} -h $URL -o multiAttributes -f $INPUT_DIR/${TENANT}_entitlement_calculate_split_opportunity_with_matched_offers_resolve_as_win_${QTRX}.csv -a "-s app.opportunity -o update" &> $LOG_DIR/${TENANT}_entitlement_calculate_split_opportunity_with_matched_offers_resolve_as_win_${QTRX}.log
	CHECK_ERROR $? "Step 3"
fi 

if [[ "${MODE}" == "ALL" || "${MODE}" == "SPLIT" ]]
then
	echo "===$(date) Step 4==="
    ./executeTillCompletion.sh -t ${TENANT} -h $URL -o splitOpportunities -f $INPUT_DIR/${TENANT}_entitlement_split_opportunity_with_unmatched_offers_${QTRX}.csv -a "--revert true" &> $LOG_DIR/${TENANT}_entitlement_split_opportunity_with_unmatched_offers_${QTRX}.log
	CHECK_ERROR $? "Step 4"
fi

if [[ "${MODE}" == "ALL" || "${MODE}" == "CLOSE" ]]
then
	echo "===$(date) Step 5==="
    ./executeTillCompletion.sh -t ${TENANT} -h $URL -o resolveAsSuccess -f $INPUT_DIR/${TENANT}_entitlement_resolve_as_win_${QTRX}.csv -a " --zenMode true" &> $LOG_DIR/${TENANT}_entitlement_resolve_as_win_${QTRX}.log
	CHECK_ERROR $? "Step 5"
fi

echo "===$(date) Processing complete."
