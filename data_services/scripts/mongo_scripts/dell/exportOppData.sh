if [[ "$1" == "" || "$2" == "" ]]; then
   echo "Usage: $0 <buid> <tag> "
   echo "  France: 909, Ireland: 5102"
   exit 1
fi

buid=$1
qtr=$2
#shift
#tags="["

#while (( "$#" )); do
#tags=$tags" '"$1"',"
#shift
#done
#tags=$tags"]"

output="../../reports"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

echo "Extracting opportunities to $output ..."
mongo --quiet testdata --eval "var buId='${buid}'; var qtr='${qtr}';" jj_opportunity_extract.js  > ${output}/${buid}.${qtr}.opps.out

