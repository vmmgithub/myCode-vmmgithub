if [[ "$1" == "" ]]; then
   echo "Usage: $0 <buId> "
   exit 1
fi

buId=$1

mongo --quiet testdata --eval "var buId='${buId}'; " updateEOSDate.js  > ../../reports/${buId}.updateEOSDate.out
