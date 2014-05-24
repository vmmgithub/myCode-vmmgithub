if [[ "$1" == "" || "$2" == "" ]]; then
   echo "Usage: $0 <buid> <qtr> "
   echo "  France: 909, Ireland: 5102"
   exit 1
fi

buid=$1
#shift
#tags="["
#
#while (( "$#" )); do
#tags=$tags" '"$1"',"
#shift
#done
#tags=$tags"]"
qtr=$2

output="../../reports"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

echo "Extracting offers to $output ..."
mongo --quiet testdata --eval "var buId='${buid}'; var qtr='${qtr}';" jj_offer_extract.js > ${output}/${buid}.${qtr}.offers.out

