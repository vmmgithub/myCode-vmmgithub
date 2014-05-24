#!/bin/bash --

# 1 Service Asset Name
# 2 Service Asset UID   
# 3 Primary
# 4 Asset Tag
# 5 Contract Number
# 6 End Date
# 7 Batch Quarter
# 8 Ship Date
# 9 Segment Code
# 10 Service Class
# 11 Covered EOS
# 12 Product EOS
# 13 Customer Id
# 14 Covered Id
# 15 Has Opp
# 16 Customer UID
# 17 Covered Id
# 18 Product Id
# 19 Product UID
# 20 Affinity Id
# 21 Affinity Path

if [[ "$1" == "" ]]; then
   echo "Usage: $0 <buid> regen <startDate> <endDate>"
   echo "  France: 909, Ireland: 5102"
   exit 1
fi

buid=$1
regen="$2"
startDate=$3
endDate=$4

if [[ -z $startDate ]]
then
startDate="2010-08-04T00:00:00Z"
fi

if [[ -z $endDate ]]
then
endDate="2023-11-01T00:00:00Z"
fi

output="../../reports"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

out="${output}/${buid}.service_asset_extract.out"

echo "Extracting data for ${buid} with ${startDate} and ${endDate} and dumping files into ${output}"

if [[ ! -z $regen ]]
then 
echo "Extracting service assets ..."
mongo --quiet testdata --eval "var buid='${buid}';var startDate='${startDate}';var endDate='${endDate}';" service_asset_extract.js > ${out}
fi

