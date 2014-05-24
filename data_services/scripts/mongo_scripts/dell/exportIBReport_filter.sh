if [[ "$1" == "" ]]; then
   echo "Usage: $0 <buid> "
   echo "  France: 909, Ireland: 5102"
   exit 1
fi

buid=$1

output="../../reports"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

echo "Extracting IB reports to $output ..."
# mongo --quiet testdata --eval "var buId='${buid}';" jj_ib_customers.js  > ${output}/${buid}.IBReport.out
mongo --quiet testdata --eval "var buId='${buid}';" ib_report_extract_filter.js  > ${output}/${buid}.IBReport.out
echo "DONE"
