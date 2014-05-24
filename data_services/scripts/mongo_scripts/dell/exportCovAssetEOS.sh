if [ "$1" == "" ]; then
   echo "Usage: $0 <buid> "
   echo "  France: 909, Ireland: 5102"
   exit 1
fi

buid=$1
#shift
#tags="["

#while (( "$#" )); do
#tags=$tags" '"$1"',"
#shift
#done
#tags=$tags"]"

output="../../reports"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

echo "Extracting Covered Assets to $output ..."
mongo --quiet testdata --eval "var buId='${buid}';" covAsset_EOS_extract.js  > ${output}/${buid}.covAsst.out
