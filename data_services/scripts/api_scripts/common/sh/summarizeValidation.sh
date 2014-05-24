logfile=$1
if [[ -z $logfile ]]
then
	echo "Usage is $0 logfile inputfile"
	exit 1;
fi

inputfile=$2
if [[ -z ${inputfile} ]]
then
	inputfile=`head -1 "${logfile}" | cut -d' ' -f3`
fi
tmp="${logfile}.tmp"

total=`cat "${inputfile}" | wc -l`
echo "NUMBER OF INPUT LINES ... ${total}"

prc=`egrep 'error|info' "${logfile}" | wc -l`
echo "NUMBER OF PROCESSED LINES ... ${prc}"

exc=`grep 'error' "${logfile}" | wc -l`
echo "NUMBER OF EXCEPTIONS ... ${exc}"

for check in 'ORPHAN' 'RESDT' 'AMOUNT' 'ASSETCOUNT' 'SALESREP' 'STAGE'
do
	c=`grep 'warn' "${logfile}" | grep "${check}" | wc -l`
	echo "NUMBER OF EXCEPTIONS FOR ${check} ... ${c}"
	if [[ "${c}" -gt 0 ]]
	then
		grep 'warn' "${logfile}" | grep "${pattern}"  | cut -d']' -f3 | cut -d',' -f1 | cut -d'"' -f2  > "${tmp}"
		head -1 "${inputfile}" > "${logfile}.${check}"
		grep -f "${tmp}" "${inputfile}" >> "${logfile}.${check}"
		rm "${tmp}"
	fi 
done

c=`grep 'warn' "${logfile}" | grep ',"STAGE",'| awk -F"," '{print $2,$3,$4}'| sort | uniq -c | sort -n -r`
echo "BREAKDOWN OF SALES STAGE EXCEPTIONS"
echo "${c}"


