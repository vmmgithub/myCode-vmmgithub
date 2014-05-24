if [[ "$1" == "" || "$2" == "" ]]; then
   echo "Usage: $0 <tenant> [empty|notContacted|complete|displayName] <qtr> <test>"
   exit 1
fi

tenant=$1
mode=$2
qtr=$3
test=$4
output="../../reports"
log="../../reports/${tenant}.${mode}.undoOppGen.log"

case "$mode" in

'empty')
	incomplete=false
	deleteOnlyEmpty=true
	notContacted=false
	displayName=false
	;;
'notDetermined')
	incomplete=false
	deleteOnlyEmpty=true
	notContacted=false
	displayName=false
	;;
'notContacted')
	incomplete=true
	deleteOnlyEmpty=false
	notContacted=true
	displayName=false
	;;
'displayName')
	incomplete=false
	deleteOnlyEmpty=false
	notContacted=false
	displayName=qtr
	;;
'complete')
	incomplete=false
	deleteOnlyEmpty=false
	notContacted=false
	displayName=false
	;;
*) 
	echo "Invalid mode [empty|notDetermined|notContacted|complete|displayName]"
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

mongo testdata --quiet undoOppGen.js --eval "var displayName='${displayName}'; var tenant = '${tenant}'; var incomplete = ${incomplete}; var deleteOnlyEmpty = ${deleteOnlyEmpty}; var mockRun = ${mockRun}; var qtr = '${qtr}'; var notContacted = ${notContacted}; var tags = [];"   >> ${log} 
echo "Logs are at ${log}"
