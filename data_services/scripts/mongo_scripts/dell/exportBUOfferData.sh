if [[ "$1" == "" || "$2" == "" ]]; then
   echo "Usage: $0 <buid> <tag> "
   echo "  France: 909, Ireland: 5102"
   exit 1
fi

buid=$1
tags=$2
output="../../reports"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

echo "Extracting offers to $output ..."
mongo --quiet testdata --eval "var tags='${tags}';" offer_extract.js > ${output}/${buid}.offers.out

