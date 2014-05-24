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
startDate="2013-11-02T00:00:00Z"
fi

if [[ -z $endDate ]]
then
endDate="2014-01-31T00:00:00Z"
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

echo "Extracting customers for ${buid}"
ref_cust="${output}/${buid}.referenced_customer.out"
ref_cust_tmp="${output}/${buid}.referenced_customer.tmp"
echo "var keys= [" > ${ref_cust}
cut -f 2,13 ${out} | tail -n +2 | grep -v "undefined" | cut -f 2 | sort -u | sed "s/^/\"/;s/\$/\",/" >> ${ref_cust}
echo "''];" >> ${ref_cust}
echo "" >> ${ref_cust}
echo "var uids= [" >> ${ref_cust}
cut -f 2,16 ${out} | tail -n +2 | grep -v "undefined" | cut -f 2 | sort -u | sed "s/^/\"/;s/\$/\",/" >> ${ref_cust}
echo "''];" >> ${ref_cust}

mongo --quiet testdata --eval "var file='${ref_cust}';" customer_extract.js > ${ref_cust_tmp}
cut -f 2,13,16 ${out} | grep "undefined" | cut -f 1,3 | grep -v "undefined" > ${output}/${buid}.unlinked_customer.out
grep -F "::Dangling::" ${ref_cust_tmp} | cut -d' ' -f2 > ${output}/${buid}.dangling_customer.out
grep -v "::Dangling::" ${ref_cust_tmp} > ${output}/${buid}.customer.out
rm ${ref_cust}
rm ${ref_cust_tmp}

echo "Extracting covered assets for ${buid}"
ref_cov="${output}/${buid}.referenced_covered.out"
ref_cov_temp=" ${output}/${buid}.referenced_covered.tmp"
echo "var keys= [" > ${ref_cov}
cut -f 2,14 ${out} | tail -n +2 | grep -v "undefined" | cut -f 2 | sort -u | sed "s/^/\"/;s/\$/\",/" >> ${ref_cov}
echo "''];" >> ${ref_cov}
echo "" >> ${ref_cov}
echo "var uids= [" >> ${ref_cov}
cut -f 2,17 ${out} | tail -n +2 | grep -v "undefined" | cut -f 2 | sort -u | sed "s/^/\"/;s/\$/\",/" >> ${ref_cov}
echo "''];" >> ${ref_cov}
echo "" >> ${ref_cov}

mongo --quiet testdata --eval "var file='${ref_cov}'; var coll='app.assets';" covered_asset_extract.js > ${ref_cov_temp}
cut -f 2,14,17 ${out} | grep "undefined" | cut -f 1,3 > ${output}/${buid}.unlinked_covered.out
grep -F "::Dangling::" ${ref_cov_temp} | cut -d' ' -f2 > ${output}/${buid}.dangling_covered.out
grep -v "::Dangling::" ${ref_cov_temp} > ${output}/${buid}.covered.out
rm ${ref_cov_temp}
rm ${ref_cov}
