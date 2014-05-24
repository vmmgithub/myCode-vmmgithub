base=/data_raid10/workspace/IBReports

theatre=$1
type=$2
steps=$3

if [[ -z $theatre || -z $type || -z $steps ]]
then
        echo "Usage $0 <emea|apj|abu> <Assigned|Unassigned> <both|convert|upload>"
        exit 1
fi

while read bu
do
	now=`date +"%DT%T"`
	if [[ $steps == "both" || $steps == "convert" ]]
	then
		if [[ ! -d ${theatre}_csv/${type}/${bu} || ! -d ${theatre}_xls/${type}/${bu} ]]
		then
			echo "ERROR ${type} ${bu} dirs not exist"
		else
			csv=`find ${theatre}_csv/${type}/${bu} -type f | wc -l`
			xls=`find ${theatre}_xls/${type}/${bu} -type f | wc -l`
			if [[ $csv -ne $xls ]]
			then
				echo "ERROR CONVERT ${type} ${bu} mismatch ${csv} ${xls}"
			else
				echo "SUCCESS CONVERT ${type} ${bu} match ${csv} ${xls}"
			fi
		fi
	fi

	if [[ $steps == "both" || $steps == "upload" ]]
	then
		log=${base}/logs/${bu}.${type}.log	
		 if [[ ! -d ${theatre}_xls/${type}/${bu} ]]
                then
                        echo "ERROR ${type} ${bu} dirs not exist"
                else
			xls=`find ${theatre}_xls/${type}/${bu} -type f | wc -l`
			upl=`grep "attached with" ${log} | grep SUCCESS | wc -l`
			last=`tail -1 ${log} |cut -d' ' -f3`
			if [[ $upl -eq $xls || $last == "DONE" ]]
                        then
                                echo "SUCCESS UPLOAD ${type} ${bu} match ${xls} ${upl}"
                        else
                                echo "ERROR UPLOAD ${type} ${bu} mismatch ${xls} ${upl}"
                        fi
		fi
	fi
done < ${theatre}BU.lst
