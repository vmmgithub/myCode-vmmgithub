if [[ "$1" == "" || "$2" == "" ]]; then
   echo "Usage: $0 <coll> <type> "
   exit 1
fi

coll=$1
type=$2
subtype=`echo $type | cut -d'/' -f2`

if [[ -z subtype ]]
then
subtype="$type"
fi

output="../../reports"

if [[ ! -d $output ]]
then
mkdir -p $output
fi

mongo --quiet testdata --eval "var type='${type}'; var coll='${coll}';" extractUIDs.js > ${output}/${coll}.${subtype}.uids.out
