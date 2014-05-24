if [[ "$1" == "" || "$2" == "" ]]; then
   echo "Usage: $0 <buid> <qtr>"
   exit 1
fi

buid=$1
qtr=$2
output="../../reports"
log="${output}/${buid}.${mode}.undoOppGen.log"

incomplete=true
deleteOnlyEmpty=true
notContacted=false

mongo testdata --quiet undoOppGen.js --eval "var buId = '${buid}'; var qtr = '${qtr}'; var incomplete = true; var mockRun = true; var deleteOnlyEmpty = true; var notContacted = false; var tags = [];" 

