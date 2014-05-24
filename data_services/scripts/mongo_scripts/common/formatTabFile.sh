in=$1
header=$2

if [[ ! -z $header ]]
then
header="-h"
fi

~/scripts/common/convertToJSON.pl ${header} -i ${in} -o ${in}.js

#number=$3
#temp=${in}.t
#temp2=${in}.t2
#array=${in}.js
#
#if [[ ! -z $header ]]
#then
#c=`wc -l ${in} | cut -d' ' -f1`
#c=$((c-1))
#tail -${c} $in > $temp2
#else
#cp $in $temp2
#fi
#
#tr -d '\r' < ${temp2} > ${temp}
#echo "var values=[" > ${array}
#
#while read line
#do
#l1=`echo $line | cut -d' ' -f1`
#l2=`echo $line | cut -d' ' -f2`
#if [[ -z $number ]]
#then
#echo "{uid: '${l1}', value: '${l2}' }," >> ${array}
#else
#echo "{uid: '${l1}', value: ${l2} }," >> ${array}
#fi
#done < ${temp}
#
#echo "]; 
#" >> ${array}
#
#rm ${temp}
#rm ${temp2}
#mongo testdata --quiet ${array} > ${in}.log
