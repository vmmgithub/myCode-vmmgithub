#!/bin/bash --

# 1 'Service Asset ID', 
# 2 'Service Asset UID', 
# 3 'Service Asset Name', 
# 4 'Start Date', 
# 5 'End Date', 
# 6 'Batch Quarter', 
# 7 'Has Opp',
# 8 'Customer Id', 
# 9 'Customer UID' , 
# 10 'Covered Id', 
# 11 'Covered UID' , 
# 12 'Product Id', 
# 13 'Product UID',

if [[ "$1" == "" ]]; then
   echo "Usage: $0 <tenant> [startDate] [endDate]"
   exit 1
fi

tenant=$1
startDate=$2
endDate=$3
output="../../reports/${tenant}/"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

out="${output}/${tenant}.service_asset_extract.out"

echo "Extracting data for ${tenant} with ${startDate} and ${endDate} and dumping files into ${output}"

echo "Extracting service assets ..."
mongo --quiet testdata --eval "var tenant='${tenant}';var startDate='${startDate}';var endDate='${endDate}';" service_asset_extract.js > ${out}

echo "Extracting customers for ${tenant}"
ref_cust="${tenant}.referenced_customer.out"
ref_cust_tmp="${tenant}.referenced_customer.tmp"
echo "var keys= [" > ${ref_cust}
cut -f 2,8 ${out} | tail -n +2 | grep -v "undefined" | cut -f 2 | sort -u | sed "s/^/\"/;s/\$/\",/" >> ${ref_cust}
echo "''];" >> ${ref_cust}
echo "" >> ${ref_cust}
echo "var uids= [" >> ${ref_cust}
cut -f 2,9 ${out} | tail -n +2 | grep -v "undefined" | cut -f 2 | sort -u | sed "s/^/\"/;s/\$/\",/" >> ${ref_cust}
echo "''];" >> ${ref_cust}

mongo --quiet testdata --eval "var file='${ref_cust}';" customer_extract.js > ${ref_cust_tmp}
cut -f 2,8,9 ${out} | grep "undefined" | cut -f 1,3 | grep -v "undefined" > ${output}/${tenant}.unlinked_customer.out
grep -F "::Dangling::" ${ref_cust_tmp} | cut -d' ' -f2 > ${output}/${tenant}.dangling_customer.out
grep -v "::Dangling::" ${ref_cust_tmp} > ${output}/${tenant}.customer.out
rm ${ref_cust}
rm ${ref_cust_tmp}

echo "Extracting covered assets for ${tenant}"
ref_cov="${tenant}.referenced_covered.out"
ref_cov_temp=" ${tenant}.referenced_covered.tmp"
echo "var keys= [" > ${ref_cov}
cut -f 2,10 ${out} | tail -n +2 | grep -v "undefined" | cut -f 2 | sort -u | sed "s/^/\"/;s/\$/\",/" >> ${ref_cov}
echo "''];" >> ${ref_cov}
echo "" >> ${ref_cov}
echo "var uids= [" >> ${ref_cov}
cut -f 2,11 ${out} | tail -n +2 | grep -v "undefined" | cut -f 2 | sort -u | sed "s/^/\"/;s/\$/\",/" >> ${ref_cov}
echo "''];" >> ${ref_cov}
echo "" >> ${ref_cov}

mongo --quiet testdata --eval "var file='${ref_cov}'; var coll='app.assets';" covered_asset_extract.js > ${ref_cov_temp}
cut -f 2,10,11 ${out} | grep "undefined" | cut -f 1,3 > ${output}/${tenant}.unlinked_covered.out
grep -F "::Dangling::" ${ref_cov_temp} | cut -d' ' -f2 > ${output}/${tenant}.dangling_covered.out
grep -v "::Dangling::" ${ref_cov_temp} > ${output}/${tenant}.covered.out
rm ${ref_cov_temp}
rm ${ref_cov}
