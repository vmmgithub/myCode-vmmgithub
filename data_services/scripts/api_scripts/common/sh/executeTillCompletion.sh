#!/bin/bash

SCRIPT_DIR="../js"
LOG_DIR="../../../../data/prd/logs"

usage() { echo "Usage: $0 -h host -t <tenant> -o <operation = multiAttributes|resolveAsSuccess|resolveAsLoss|splitOpportunities> -f file -a addlnArgs " 1>&2; exit 1; }
log() { now=`date`; echo "[${now}] ${1} "; }

# Get the command line arg
while getopts ":t:o:f:h:a:" arg; do
    case "${arg}" in
        t) tenant=${OPTARG} ;;
        o) operation=${OPTARG} ;;
        f) file=${OPTARG} ;;
        h) host=${OPTARG} ;;
        a) addlnArgs=${OPTARG} ;;
        *) usage ;;
    esac
done
shift $((OPTIND-1))

# Globals
tenant=`echo ${tenant} | tr '[:upper:]' '[:lower:]'`

if [[ "${operation}" == "" || "${tenant}" == "" || "${host}" == "" || "${file}" == "" ]]; then
    usage;
fi

COUNT=0
COMPLETED="FALSE"
BASEFILENAME=`echo ${file} | rev | cut -d'/' -f1 | rev`
INFILE="${file}"
LOGFILE="${LOG_DIR}/${BASEFILENAME}.${COUNT}.log"

#  Creates background process for UI therefore not executing script multiple times
if [[ "${operation}" == "resolveAsSuccess" || "${operation}" == "resolveAsLoss" ]]; then
    COUNT=4;
fi

function scanAndPrep() {
    COUNT=$(($COUNT+1))

    if [[ -f ${LOGFILE} ]]; then
        failCount=`grep "^FAIL" ${LOGFILE} | wc -l | awk '{print $1}'`

        if [[ $failCount -le 1 ]]; then
            log "${LOGFILE} has no failures"
            COMPLETED="TRUE";
        else
            log "${LOGFILE} has ${failCount} failures; prepping for next iteration"
            INFILE="${file}.${COUNT}"
            grep "^FAIL" ${LOGFILE} | cut -d'|' -f2  > ${INFILE}
#            grep FAIL ${LOGFILE} | cut -d'|' -f2 --complement > ${INFILE}
            LOGFILE="${LOG_DIR}/${BASEFILENAME}.${COUNT}.log"
        fi
    else
        log "${LOGFILE} not present"
    fi
}

function runScript() {
    cmd="$SCRIPT_DIR/${operation}.js --host ${host} --tenant ${tenant} --file ${INFILE} ${addlnArgs} > ${LOGFILE}"
    now=`date '+%Y%m%d%H%M%S%N'`
    f="/tmp/${now}"

    echo "$cmd" > ${f}
    pCount=`wc -l ${INFILE}`
    log "STARTING execution for ${INFILE} with ${pCount} lines with ${cmd}"
    sh ${f}
    if [[ $? -ne 0 ]]; then log "Error in executing script $?"; fi
    log "COMPLETED execution"
    rm ${f}
}

# checking for failed records if any

while [[ ${COMPLETED} == "FALSE" &&  ${COUNT} -lt 5 ]]
do
     scanAndPrep
     runScript
done

log "DONE"
