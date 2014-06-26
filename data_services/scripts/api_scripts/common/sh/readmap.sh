#!/bin/bash

# Globals, unfortunately have to be declared by hand
os=(opportunities offers quotes bookings lineitems assets contacts products lookups)
objs=()
opportunities=()
offers=()
quotes=()
bookings=()
lineitems=()
assets=()
contacts=()
products=()
lookups=()

readmap() {
  file="$1"
  echo "reading from ${1} "

  while read line
  do
    coll=`echo $line | cut -d'|' -f1`
    col=`echo $line | cut -d'|' -f2`

    case $coll in 
      opportunities) opportunities=("${opportunities[@]}" ${col});;
      quotes) quotes=("${quotes[@]}" ${col});;
      bookings) bookings=("${bookings[@]}" ${col});;
      offers) offers=("${offers[@]}" ${col});;
      assets) assets=("${assets[@]}" ${col});;
      lineitems) lineitems=("${lineitems[@]}" ${col});;
      contacts) contacts=("${contacts[@]}" ${col});;
      products) products=("${products[@]}" ${col});;
      lookups) lookups=("${lookups[@]}" ${col});;
      *) log "ignoring line with a bad prefix ${line}" ;;
    esac

  done < "${file}";
}

# prepare summary map file
now=`date '+%Y%m%d%H%M%S%N'`
f="/tmp/${now}"

# Default columns that are available for all tenants
if [[ ! -z "${1}" && -f "${1}" ]]; then
  cat ${1} >> ${f}
fi

# Add tenant specific columns from columnfile, if available
if [[ ! -z "${2}" && -f "${2}" ]]; then
  cat ${2} >> ${f}
fi

## Initialize list of objects based on the map files
for coll in "${os[@]}"; do
  count=`grep ${coll} ${f} | wc -l | awk '{print $1}'`
  if [[ ${count} != 0 ]]; then
    objs=("${objs[@]}" "${coll}")
  fi
done

mapfile="/tmp/${now}.map"
cat ${f} | sort | uniq > "${mapfile}"

readmap "${mapfile}"
