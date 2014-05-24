if [[ "$1" == "" || "$2" == "" ]]; then
   echo "Usage: $0 <file> <coll> "
   exit 1
fi

file=$1
coll=$2

echo "Preparing the file... "
../common/formatTabFile.sh ${file} 

mongo --quiet testdata --eval "var file='${file}.js'; var coll='${coll}';" updateTargetAmounts.js  > ${file}.out
rm ${file}.js

