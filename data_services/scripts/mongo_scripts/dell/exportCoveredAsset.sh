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

echo "Extracting covered assets for ${buid}"
ref_cov="${buid}.referenced_covered.out"
ref_cov_temp=" ${buid}.referenced_covered.tmp"
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
