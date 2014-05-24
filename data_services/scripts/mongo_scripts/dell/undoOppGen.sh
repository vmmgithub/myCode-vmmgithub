if [[ "$1" == "" || "$2" == "" || "$3" == "" ]]; then
   echo "Usage: $0 <buid> [empty|notContacted|complete|emptyNotContNotDet] <qtr> <test>"
   exit 1
fi

buid=$1
mode=$2
qtr=$3
test=$4
output="../../reports"
log="../../reports/${buid}.${mode}.undoOppGen.log"

case "$mode" in

'empty')
        incomplete=true
        deleteOnlyEmpty=true
        notContacted=false
        ;;
'notContacted')
        incomplete=true
        deleteOnlyEmpty=false
        notContacted=true
        ;;
'complete')
        incomplete=false
        deleteOnlyEmpty=false
        notContacted=false
        ;;
'emptyNotContNotDet')
        incomplete=true
        deleteOnlyEmpty=true
        notContacted=true
        ;;
*)
        echo "Invalid mode [empty|notContacted|complete|emptyNotContNotDet]"
        exit 1;
        ;;
esac

case "$test" in

'real')
        mockRun=false
        echo "Running for real" >> ${log}
        ;;
*)
        mockRun=true
        echo "Running in test only mode"  >> ${log}
        ;;
esac

mongo testdata --quiet undoOppGen.js --eval "var buId = '${buid}'; var incomplete = ${incomplete}; var deleteOnlyEmpty = ${deleteOnlyEmpty}; var mockRun = ${mockRun}; var qtr = '${qtr}'; var notContacted = ${notContacted}; var tags = [];"   >> ${log}
