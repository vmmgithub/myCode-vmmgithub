#!/bin/bash

# Globals, unfortunately have to be declared by hand
os=(opportunities offers quotes bookings lineitems assets contacts products lookups);
objs=();
opportunities=(); maps_opportunities=();
offers=(); maps_offers=();
quotes=(); maps_quotes=();
bookings=(); maps_bookings=();
lineitems=(); maps_lineitems=();
assets=(); maps_assets=();
contacts=(); maps_contacts=();
products=(); maps_products=();
lookups=(); maps_lookups=();

readmap() {
  file="$1"
  echo "reading from ${1} "

  while read line
  do
    coll=`echo $line | cut -d'|' -f1`;
    col=`echo $line | cut -d'|' -f2`;
    map=`echo $line | cut -d'|' -f3`;
    if [[ -z "${map}" ]]; then map="-"; fi

    case $coll in 
      opportunities) opportunities+=(${col}); maps_opportunities+=(${map}) ;;
      quotes) quotes+=(${col}); maps_quotes+=(${map}) ;;
      bookings) bookings+=(${col}); maps_bookings+=(${map}) ;;
      offers) offers+=(${col}); maps_offers+=(${map}) ;;
      assets) assets+=(${col}); maps_assets+=(${map}) ;;
      lineitems) lineitems+=(${col}); maps_lineitems+=(${map}) ;;
      contacts) contacts+=(${col}); maps_contacts+=(${map}) ;;
      products) products+=(${col}); maps_products+=(${map}) ;;
      lookups) lookups+=(${col}); maps_lookups+=(${map}) ;;
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
cat ${f} | LC_ALL=C sort | uniq > "${mapfile}"

readmap "${mapfile}"
