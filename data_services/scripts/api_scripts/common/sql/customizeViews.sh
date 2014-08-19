#!/bin/bash

usage() { echo "Usage: $0 -t <tenant> [-c <columnfile>] -s [schema] " 1>&2; exit 1; }
log() { now=`date`; echo "[${now}] ${1} "; }
runCommand() {
  ccmd=$1
  now=`date '+%Y%m%d%H%M%S%N'`
  ff="/tmp/${now}.ddl.sql"

  echo ${ccmd} > ${ff}
  sh ${ff}
  rm ${ff}
}

####################################################################################
# Get the command line arg
####################################################################################
while getopts ":t:c:s:" arg; do
    case "${arg}" in
        t) tenant=${OPTARG} ;;
        c) columnfile=${OPTARG} ;;
        s) schema=${OPTARG} ;;
        *) usage ;;
    esac
done
shift $((OPTIND-1))

if [[ -z "${tenant}" ]]; then
    usage
fi

if [[ -z "${schema}" ]]; then
  schema="${tenant}"
fi

####################################################################################
# Global steps before proceeding
####################################################################################
log "Initializing from map files ... "
. ../sh/readmap.sh '../sh/downloadAllOpps.default.map' "${columnfile}"
VIEWFILE="./standard_views.sql"
log "Completed Initializing"

now=`date '+%Y%m%d%H%M%S%N'`
f="/tmp/${now}.ddl.sql"

cat > ${f} <<EOF
  use ${schema};

EOF

# Copy the master file to start the find replace
cat ${VIEWFILE} >> ${f};

####################################################################################
# Create of tables when required
####################################################################################

log "Customizing views ... "

# Use helper script to read all the map  columns that are available for all tenants
for coll in "${objs[@]}"; do
  prefix=`echo ${coll} | cut -c1-3 | tr '[:lower:]' '[:upper:]'`
  case $coll in 
    opportunities) arr="${opportunities[@]}"; map_arr=("${maps_opportunities[@]}") ;;
    quotes) arr="${quotes[@]}"; map_arr=("${maps_quotes[@]}") ;;
    bookings) arr="${bookings[@]}"; map_arr=("${maps_bookings[@]}") ;;
    offers) arr="${offers[@]}"; map_arr=("${maps_offers[@]}") ;;
    assets) arr="${assets[@]}"; map_arr=("${maps_assets[@]}") ;;
    lineitems) arr="${lineitems[@]}"; map_arr=("${maps_lineitems[@]}") ;;
    lookups) arr="${lookups[@]}"; map_arr=("${maps_lookups[@]}") ;;
    products) arr="${products[@]}"; map_arr=("${maps_products[@]}") ;;
    contacts) arr="${contacts[@]}"; map_arr=("${maps_contacts[@]}") ;;
  esac

  i=0;
  for var in ${arr}; do
    if [[ "${var}" != relationships*keyNameType ]]; then
      s=`../js/sqlizeName.js -f "${var}"`
      m=${map_arr[$i]}

      if [[ ${m} != "-" && ! -z ${m} ]]; then
        from="${m}"
        to=`echo ${s} | cut -d' ' -f1`
        runCommand "perl -i -pe 's#.?\\\$${from}\\\$.?#${prefix}.${to}#g' ${f}"
      fi
    fi
    i=$(($i+1))
  done

done

log "START creating views for ${schema} schema ... "
mysql  < ${f}
log "COMPLETE creating views for ${schema} schema  "

log "Done"
