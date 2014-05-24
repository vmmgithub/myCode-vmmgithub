#!/bin/bash
host=$1
tenant=$2
operation=$3
mode=$4
val=$5
cmd=''
if [[ -z $mode ]]
then
     echo "Usage is $0 host tenant [both|delete] [tags|displayName|_id|all] val"
     exit 1;
fi

base="${tenant}.${host}.${mode}.${val}"
log="${base}.log"

echo "Processing undo of ${tenant} ${host} ${mode} ${val} " >> "${log}"
echo "Downloading opps " >> "${log}"

if [[ "${operation}" =  "both" ]]
then
     if [[ "${mode}" = "_id" ]]
     then
          cmd="downloadOpps.js --tenant ${tenant} --host ${host} --filter '{\"_id\":\"${val}\"}'"
     elif [[ "${mode}" = "displayName" ]]
     then
          cmd="downloadOpps.js --tenant ${tenant} --host ${host} --filter '{\"displayName\":\"${val}\"}'"
     elif [[ "${mode}" = "tags" ]] 
     then
          cmd="downloadOpps.js --tenant ${tenant} --host ${host} --filter '{\"tags\":\"${val}\"}'"
     elif [[ "${mode}" = "all" ]] 
     then
          cmd="downloadOpps.js --tenant ${tenant} --host ${host}"
     else
          echo "invalid option"
          exit 1;

     fi
     echo "$cmd>>${base}.download.csv" > cmd.sh
     echo "rm ./$tenant.app.opportunities.$host*.tmp" >> cmd.sh
     echo "rm ./cmd.sh" >> cmd.sh
     sh ./cmd.sh
fi

if [[ "${operation}" =  "both" || "${operation}" =  "delete" ]]
then

       getColumn.sh "${base}.download.csv" "book-items" "_id" "${base}.items.csv"
       # transpose to be indidvual lines
       # sort & uniq
       tr -s '|' '\n' < "${base}.items.csv"|sort|uniq > "${base}.item.csv.out"
       mv "${base}.item.csv.out" "${base}.item.csv"
       deleteObjects.js --tenant "${tenant}" --host "${host}" --file "${base}.items.csv" --source "app.lineitem" --operation removeById >> "${log}"

       echo "Deleting offers " >> "${log}"
       getColumn.sh "${base}.download.csv" "off-_id" "_id" "${base}.delOffer.csv"
       deleteObjects.js --tenant "${tenant}" --host "${host}" --file "${base}.delOffer.csv" --source "app.offer" --operation removeById >> "${log}"
       #rm "${base}.delOffer.csv"

       echo "Deleting quotes " >> "${log}"
       getColumn.sh "${base}.download.csv" "qt-_id" "_id" "${base}.delQuote.csv"
       deleteObjects.js --tenant "${tenant}" --host "${host}" --file "${base}.delQuote.csv" --source "app.quote" --operation removeById >> "${log}"
       #rm "${base}.delQuote.csv"

       echo "Deleting opps " >> "${log}"
       getColumn.sh "${base}.download.csv" "opp-_id" "_id" "${base}.delOpp.csv"
       deleteObjects.js --tenant "${tenant}" --host "${host}" --file "${base}.delOpp.csv" --source "app.opportunity" --operation removeById >> "${log}"
       #rm "${base}.delOpp.csv"

       echo "Resetting assets " >> "${log}"
       getColumn.sh "${base}.download.csv" "ass-_id" "_id" "${base}.ra.csv"
       echo "_id(string),associatedOpportunity(boolean)" > "${base}.resetAsset.csv"
       cat "${base}.ra.csv" | grep -v "_id" | awk -F"," '{print $1",false"}' >> "${base}.resetAsset.csv"
       rm "${base}.ra.csv"

       multiAttributes.js --tenant "${tenant}" --host "${host}" --file "${base}.resetAsset.csv" --source "app.asset" >> "${log}"
#       rm "${base}.resetAsset.csv"
fi

echo "Done"

