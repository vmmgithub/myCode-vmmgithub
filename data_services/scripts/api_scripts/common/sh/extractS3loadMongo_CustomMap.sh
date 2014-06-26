#!/bin/bash

if [[ "$1" == "" ]]; then
   echo "Usage: $0 <tenant>"
   exit 1
fi

temp=$1
export tenant=`echo $temp | tr '[:upper:]' '[:lower:]'`
#export file_location=$2

export Base_Dir=/home/ec2-user/
export date_var=`date +"%m_%d_%y"`
mkdir -p /storage/data/tenant/$tenant/raw_data/$date_var
mkdir -p /storage/data/tenant/$tenant/$date_var
export raw_file_path=/storage/data/tenant/$tenant/raw_data/$date_var
export output_file_path=/storage/data/tenant/$tenant/$date_var

export mongo_scripts=/data/software/Implementations/data_services/scripts/mongo_scripts/common

#cd $raw_file_path
#rm -f *

S3_file_nm=`s3cmd ls s3://ds-mongodump/$tenant/|sort|grep .tar|tail -1|rev|awk -F"/" '{ print $1 }'|rev`

if [[ $S3_file_nm ]]; then
	s3cmd get s3://ds-mongodump/$tenant/$S3_file_nm ${raw_file_path}/.
else
	echo -e "\n\nNo files to export from S3 folder!! Exiting Script\n\n"
	exit 0
fi

cd $raw_file_path
tar -xf $S3_file_nm -C ${raw_file_path} --strip-components=1
mv ${raw_file_path}/export/* ${raw_file_path}/
gunzip *.json.gz

ls -1rt *.json > filenames
while read f
do
	grep -v -e '^connected to:' -e '^exported' $f > tmp
	mv tmp $f
done < filenames


cd
source .bash_profile

conn_chk=$?
if [[ $conn_chk -ne 0 ]]; then
	echo -e "\n\nDid not enter Mongo Shell !!! Exiting Script !!!\n\n"
	exit 0
else
	echo -e "\n\nEntered Mongo Shell !!!\n\n" 
fi

mongo testdata --eval "db.dropDatabase()"

echo -e "\nStarting import of entities !!!\n\n"


objs=(opportunities offers quotes bookings lineitems assets)

for coll in "${objs[@]}"
do
	 echo -e "\n\nImporting ${coll}\n\n"
         mongoimport --db testdata --collection app.${coll} --type json --file ${raw_file_path}/*app.${coll}.json

done

echo -e "\n\nAll entities successfully imported into Mongo !!! Terminating Mongo Shell !!!\n\n"

echo -e "\nStarting Creation of MySQL Load ready files !!! \n\n"

cd ${mongo_scripts}

objs=(opportunities offers quotes bookings lineitems assets)

for coll in "${objs[@]}"
do
        echo -e "\nExporting app.${coll} \n\n"
	column_string=`grep "$col1" $Base_Dir/$tenant/$tenant.custom.map |sed 's/$/,/g'|tr -cd "[:print:]"|sed 's/,\+$//'`
	
        mongo testdata --quiet --eval "var tenant='${tenant}'; var coll='app.${coll}'" exportCollection_sas.js $column_string > "${output_file_path}/${coll}.both.out"
	
	if [[ $? -ne 0 ]]; then
		echo "Error in unloading data from Mongo for tenant ${tenant} for app.${coll} \n"
		exit 0
	fi

        grep -v RELATIONSHIPROWS "${output_file_path}/${coll}.both.out" > "${output_file_path}/${coll}.out"
        grep RELATIONSHIPROWS "${output_file_path}/${coll}.both.out" | cut -d'|' -f2 >> "${output_file_path}/RELATIONSHIPS.out"
        rm "${output_file_path}/${coll}.both.out"
done

echo -e "\n\nAll entities successfully Exported from Mongo !!! Terminating Mongo Shell !!!\n\n"

#s3cmd put --add-header=x-amz-server-side-encryption:AES256 "${zip}" "s3://Renew-Dev-Workspace/nithin/${tenant}/"
