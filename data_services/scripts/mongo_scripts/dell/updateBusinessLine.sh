if [[ "$1" == "" ]]; then
   echo "Usage: $0 <file> "
   exit 1
fi

file=$1

echo "Preparing the file... "
../common/formatTabFile.sh ${file} 

mongo --quiet testdata --eval "var file='${file}.js'; " updateBusinessLine.js  > ${file}.out
rm ${file}.js
