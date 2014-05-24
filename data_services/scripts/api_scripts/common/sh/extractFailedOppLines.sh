logfile=$1
if [[ -z $logfile ]]
then
	echo "Usage is $0 logfile [patter] [from] [to] "
	exit 1;
fi

inputfile=`head -1 "${logfile}" | cut -d' ' -f3`
tmp="${logfile}.tmp"
now=`date '+%Y%m%d%H%M'`
out="${logfile}.${now}.csv"

pattern=$2
from=$3
to=$4

if [[ -z ${pattern} ]]
then
	head -1 "${inputfile}" > "${out}"
	grep 'debug' "${logfile}" | cut -d']' -f3 >> "${out}"
else
	grep 'warn' "${logfile}" | grep "${pattern}" | grep "${from}" | grep "${to}" | cut -d']' -f3 | cut -d',' -f1 | cut -d'"' -f2  > "${tmp}"
	head -1 "${inputfile}" > "${out}"
	grep -f "${tmp}" "${inputfile}" >> "${out}"
fi

ct=`wc -l "${out}" | cut -d' ' -f1`
echo "Output is at ${out} and has ${ct} lines"

