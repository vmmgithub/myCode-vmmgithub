#!/bin/bash

usage() { echo "Usage: $0 -h <hostname> -t <tenant> -o <outputdir> [-f <jsonfilter>] [-c <columnfile>] " 1>&2; exit 1; }
log() { now=`date`; echo "[${now}] ${1} "; }

# Get the command line arg
while getopts ":h:t:o:f:c:" arg; do
    case "${arg}" in
        h) host=${OPTARG} ;;
        t) tenant=${OPTARG} ;;
        o) output=${OPTARG} ;;
        f) filter=${OPTARG} ;;
        c) columnfile=${OPTARG} ;;
        *) usage ;;
    esac
done
shift $((OPTIND-1))

if [[ -z "${host}" || -z "${tenant}" || -z "${output}" ]]; then
    usage
fi

# Create the output directory, if it does not exist
mkdir -p ${output}
cat /dev/null > ${output}/RELATIONSHIPS.out

# Use helper script to read all the map  columns that are available for all tenants
log "Initializing from map files ... "
. ./readmap.sh 'downloadAllOpps.default.map' "${columnfile}"
log "Completed Initializing"

# Money maker that goes through each collection setup by the helper and downloads it
# Start downloading objects one collection at a time
for coll in "${objs[@]}"; do
  if [[ ${coll} == "contacts" ]]; then fullCollName="core.${coll}"; else fullCollName="app.${coll}"; fi
  cmd="../js/downloadObjects.js --tenant ${tenant} --host ${host} --source ${fullCollName} --db true --log true"

  if [[ ! -z ${filter} ]]; then
    cmd="${cmd} --searchBy \"${filter}\""
  fi

  case ${coll} in 
    opportunities) arr="${opportunities[@]}";;
    quotes) arr="${quotes[@]}";;
    bookings) arr="${bookings[@]}";;
    offers) arr="${offers[@]}";;
    assets) arr="${assets[@]}";;
    lineitems) arr="${lineitems[@]}";;
    lookups) arr="${lookups[@]}";;
    products) arr="${products[@]}";;
    contacts) arr="${contacts[@]}";;
 esac

  for var in ${arr}; do
    cmd="$cmd --columns \"${var}\""
  done

  cmd="$cmd > ${output}/${coll}.both.out"
  now=`date '+%Y%m%d%H%M%S%N'`
  f="/tmp/${now}"

  echo "$cmd" > ${f}
  log "STARTING extraction $cmd"
  sh ${f}
  rm ${f}

  grep -v RELATIONSHIPROWS ${output}/${coll}.both.out > ${output}/${coll}.out
  grep RELATIONSHIPROWS ${output}/${coll}.both.out | cut -d'|' -f1 --complement >> ${output}/RELATIONSHIPS.out
  rm ${output}/${coll}.both.out
  objects=`wc -l ${output}/${coll}.out | awk {'print $1'}`
  relationships=`wc -l ${output}/RELATIONSHIPS.out | awk {'print $1'}`
  log "COMPLETED with ${coll} extract with ${objects} objects and ${relationships} relationships"
  log
done

log "Done"
