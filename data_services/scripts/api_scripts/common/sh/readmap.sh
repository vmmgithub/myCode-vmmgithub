#!/bin/bash

# Globals
objs=(opportunities offers quotes bookings lineitems assets)
opportunities=()
offers=()
quotes=()
bookings=()
lineitems=()
assets=()

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
      *) log "ignoring line with a bad prefix ${line}" ;;
    esac

  done < "${file}";
}

# Default columns that are available for all tenants
if [[ ! -z "${1}" && -f "${1}" ]]; then
  readmap "${1}"
fi

# Add tenant specific columns from columnfile, if available
if [[ ! -z "${2}" && -f "${2}" ]]; then
  readmap "${2}"
fi

