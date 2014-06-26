tenant=$1
master=$2

log() { now=`date`; echo "[${now}] ${1} "; }

if [[ "$tenant" == "" || "$master" == "" ]]; then
	echo "Usage: $0 <tenant> <masterfile>" 1>&2; 
	exit 1;
fi

log "STARTING - ${tenant} export "

rm ${tenant}/*.tar
rm ${tenant}/export/*
mkdir -p ${tenant}/export/

now=`date '+%Y%m%d%H%M%S%N'`
f="/tmp/${now}"

# hard coded list of collections
objs=(opportunities offers quotes bookings lineitems assets products lookups contacts)

for coll in "${objs[@]}"
do

	cat ${master} | grep "${coll}" | cut -d'|' -f2 | cut -d'.' -f1 > ${tenant}/${coll}.master
	count=`wc -l ${tenant}/${coll}.master | awk '{print $1}'`
	if [[ ${count} != 0 ]]
	then
		if [[ ${coll} == "contacts" ]]; then fullCollName="core.${coll}"; else fullCollName="app.${coll}"; fi

		cat > ${f} <<EOF
		mongoexport -d testdata -c ${fullCollName}  --query '{"systemProperties.tenant":"${tenant}", "systemProperties.expiredOn": new Date(253370764800000)}' --fieldFile ${tenant}/${coll}.master  | gzip > ${tenant}/export/app.${coll}.json.gz
EOF
	    log "STARTING extraction ${coll}"
	    sh ${f}
	    log "COMPLETED extraction ${coll}"
	fi
done

tar -cf ${tenant}/$1.tar ${tenant}/export
s3cp ${tenant}/$1.tar s3://ds-mongodump/uat/${tenant}/$1.tar
log "COMPLETED - ${tenant} export"
