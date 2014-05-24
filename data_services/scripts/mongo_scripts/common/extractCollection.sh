#!/bin/bash

if [[ "$2" == "" ]]; then
   echo "Usage: $0 <tenant> <output>"
   exit 1
fi

tenant=$1
output=$2
zip="${output}".tar.gz

mkdir -p "${output}"
#rm ${output}/*
objs=(opportunities offers quotes bookings lineitems assets)

for coll in "${objs[@]}"
do
        mongo testdata --quiet --eval "var tenant='${tenant}'; var coll='app.${coll}'" exportCollection.js > "${output}/${coll}.both.out"
        grep -v RELATIONSHIPROWS "${output}/${coll}.both.out" > "${output}/${coll}.out"
        grep RELATIONSHIPROWS "${output}/${coll}.both.out" | cut -d'|' -f2 >> "${output}/RELATIONSHIPS.out"
        rm "${output}/${coll}.both.out"
done

tar cfz "${zip}" "${output}"
rm "${output}"/*
rmdir "${output}"

s3cmd put --add-header=x-amz-server-side-encryption:AES256 "${zip}" "s3://Renew-Dev-Workspace/nithin/${tenant}/"
