theater=$1
buid=$2
type=$3
host=$4
start=$5
base=/data_raid10/workspace/IBReports/${theater}_xls

if [[ -z $buid || -z $type ]]
then
	echo "Usage $0 <emea|apj|abu> <buid> <Assigned|Unassigned> host [start]"
	exit 1;
fi

./attachIBReports.js attachDocuments -h $host -p 443 -t dell -d ${base}/${type}/${buid} -i ${start} -u 'bill.moor@dell.com'

