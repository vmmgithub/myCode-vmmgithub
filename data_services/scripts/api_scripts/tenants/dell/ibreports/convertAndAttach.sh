base=/data_raid10/workspace/IBReports
uploader=/data_raid10/software/dell-export/node-Dell/Extractor/scrub_stage/general/
host=dell-prd1dl2-int.ssi-cloud.com

theater=$1
buid=$2
type=$3
steps=$4
start=$5

indir=${base}/${theater}_csv/${type}/${buid}
outdir=${base}/${theater}_xls/${type}/${buid}
log=${base}/logs/${buid}.${type}.log

if [[ -z $buid || -z $steps ]]
then
	echo "Usage $0 <emea|apj|abu> <buid> <Assigned|Unassigned> <both|convert|upload> [startNumber]"
	exit 1
fi

if [[ -z $start ]]
then
	start=1
fi

if [[ -f $log ]]
then
	now=`date +"%Y%m%d_%H%M%S"`
	mv ${log} ${log}.bkup.${now}
fi

i=1
echo "Processing ${buid} ..." > ${log}
if [[ $steps == "both" || $steps == "convert" ]]
then
	echo "Step 1: converting from csv to xlsx" >> ${log}
	cd ${indir}
	mkdir -p ${outdir}
	rm -rf ${outdir}/*

	if [ "$(ls -A ${indir})" ]; then
		for file in *.csv
		do
			i=`expr $i + 1`
			if [[ $i -ge $start ]]
			then
				name=`echo $file | cut -d'.' -f1`
				now=`date +"%DT%T"`
				tmp=/tmp/${file}.tmp
				echo "[${now}] Processing $file" >> ${log}
				cp $file ${tmp}
				perl -i -pe 's#\\"##g' ${tmp}
				perl -i -pe 's#\\##g' ${tmp}
				${base}/csv2xls ${tmp} ${outdir}/${name}.xlsx >> ${log}
				rm ${tmp}
			fi
		done 2>> ${log}
	fi
fi

if [[ $steps == "both" || $steps == "upload" ]]
then
	echo "Step 2: attaching xlsx to the contact" >> ${log}
	cd ${uploader}
	./attach.sh ${theater} ${buid} ${type} ${host} ${start} >> ${log}
fi
