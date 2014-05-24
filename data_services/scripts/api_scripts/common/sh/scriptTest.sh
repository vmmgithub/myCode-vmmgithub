tenant=nexmech
host="stgcurrent.ssi-cloud.com"
user="bill.moor@${tenant}.com"
passwd="passwordone"
port=443
param1="_id"
param2="displayName"
param3="relationships.salesRep"
jsPath="/data_raid10/software/Implementations/data_services/scripts/api_scripts/common/js"
log="/tmp"

$jsPath/downloadObjects.js -t ${tenant}  -h ${host} -u ${user} -p ${passwd} -n ${port} -l ${param1} -l ${param2}  -l ${param3} > $log/${tenant}Opp.csv

file="$log/scriptError"

if [[ -f "$file" ]] 
then 
     rm $file 
fi

awk '{ handled = 0 }
      /Error/ {  handled = 1}
      { if (handled) print "Error in download object" > "$log/scriptError" } ' $log/${tenant}Opp.csv 
 
if [[ -f "$file"  ]]
then
      exit;
else
   echo "Name,Field Name" > $log/${tenant}ManageAttrib.csv
   awk -F'\",\"|\"|\"$|:' '{print $2",\""$3"_test\""}' $log/${tenant}Opp.csv|grep -v -e "_id" -e "undefined" | head  >> $log/${tenant}ManageAttrib.csv

   echo "_id(string),displayName(string)" > $log/${tenant}MultiAttrib.csv
   awk -F'\",\"|\"|\"$|:' '{print $2",\""$3"\""}' $log/${tenant}Opp.csv|grep -v -e "_id" -e "undefined" | head  >> $log/${tenant}MultiAttrib.csv
 
   echo "Source,Target" > $log/${tenant}Relation.csv
   awk -F'\",\"|\"|\"$|:' '{if ($4) print $2","$4 }' $log/${tenant}Opp.csv|grep -v -e "_id" -e "undefined"| head  >> $log/${tenant}Relation.csv 
fi

$jsPath/manageRelations.js -t ${tenant}  -h ${host} -u ${user} -p ${passwd} -n ${port} -b ${param1} -c ${param1}  -v true -o remove -f  $log/${tenant}Relation.csv> $log/${tenant}Relations.remove.log

$jsPath/manageRelations.js -t ${tenant}  -h ${host} -u ${user} -p ${passwd} -n ${port} -b ${param1} -c ${param1}  -v true -o add -f  $log/${tenant}Relation.csv> $log/${tenant}Relations.add.log

$jsPath/manageAttributes.js -t ${tenant}  -h ${host} -u ${user} -p ${passwd} -n ${port} -b ${param1} -e ${param2} -o update -f  $log/${tenant}ManageAttrib.csv> $log/${tenant}ManageAttrib.log

$jsPath/multiAttributes.js -t ${tenant}  -h ${host} -u ${user} -p ${passwd} -n ${port} -o update -f  $log/${tenant}MultiAttrib.csv> $log/${tenant}MultiAttrib.log



