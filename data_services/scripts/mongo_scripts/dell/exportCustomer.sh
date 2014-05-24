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

if [[ "$1" == ""  || "$2" == "" ]]; then
   echo "Usage: $0 <buid> <serviceAssetFile> "
   echo "  France: 909, Ireland: 5102"
   exit 1
fi

buid=$1
out="$2"

output="../../reports"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

echo "Extracting data for ${buid} with ${startDate} and ${endDate} and dumping files into ${output}"

echo "Extracting customers for ${buid}"
ref_cust="${buid}.referenced_customer.out"
ref_cust_tmp="${buid}.referenced_customer.tmp"
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

