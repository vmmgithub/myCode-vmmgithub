file=$1
columnHeader=$2
newHeader=$3
newfile=$4

if [[ -z $newfile ]]
then
	echo "Usage is $0 file columnHeader newHeaderName newfile"
	exit 1;
fi

echo "${newHeader}" > "${newfile}"

string=`head -1 "${file}"`
IFS=',' read -a array <<< "$string"
columnIndex=""

for index in "${!array[@]}"
do
	if [[ "${array[index]}" == "${columnHeader}" || "${array[index]}" == "\"${columnHeader}\"" ]]
	then
		columnIndex=$(($index+2))
	fi
done

if [[ ! -z  ${columnIndex} ]]
then
	#cut -d',' -f${columnIndex} "${file}" | sort | uniq | grep -v "${columnHeader}" >> "${newfile}"
	cmd="grep -v \"${columnHeader}\" \"${file}\" |  awk -F'\",\"|^\"|\"$' '{print \$${columnIndex}}' | sort | uniq >> \"${newfile}\""
	now=`date '+%Y%m%d%H%M%S%N'`
	f="/tmp/${now}"
	echo ${cmd} > ${f}
	chmod +x ${f}
	${f}
	rm ${f}
else
	echo "Column not found ${columnHeader}"
fi
