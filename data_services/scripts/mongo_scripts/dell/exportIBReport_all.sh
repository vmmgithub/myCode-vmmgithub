output="../../reports"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

echo "Extracting IB reports to $output ..."

while read buid
do
now=`date +"%DT%T"`
echo "[${now}] Extracting IB report for ${buid}"
mongo --quiet testdata --eval "var buId='${buid}';" ib_report_extract_filter.js  > ${output}/${buid}.IBReport.out
done < BU.lst
echo "DONE ALL"
