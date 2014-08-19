#!/bin/bash

INT_DATE=$1

  if [[ $INT_DATE ]]; then
	echo -e "\n   DE Files would be extracted for $INT_DATE"
  else
	echo -e "\n Input date has to be provided in YYYY-MM-DD format"
	echo -e "\n Exiting script"
	exit 1
  fi

inp_dir=/data/software/Implementations/data_services/bookings/tenants/polycom/data/boomi_load
script_dir=/data/software/Implementations/data_services/bookings/tenants/polycom/sh
ext_dir=/data/workspace/polycom/extracts
dir_date=`date +"%m_%d_%y"`
date_var=`date +"%m_%d_%y_%H%M%S"`
user=smukerji@servicesource.com
pass=x1xbjep
server=74.201.119.36
lftp_tmp_loc="/'Renew Data Services'/'Boomi Data'/Tenant/Polycom/tmp"
lftp_act_loc="/'Renew Data Services'/'Boomi Data'/Tenant/Polycom"

#Check if there are any files from previous runs:

echo -e " Check if there are any files from previous run: \n"
   if stat --printf='' $inp_dir/*.csv 2>/dev/null; then
   
	mkdir -p $inp_dir/$dir_date
	mv $inp_dir/*.csv $inp_dir/$dir_date/
	mv $inp_dir/*.txt $inp_dir/$dir_date/
	echo -e "  All .csv file have been moved to folder $dir_date \n"
  else
	echo -e " No .csv files to move \n"

  fi

#Reading the Stored Proc List file

  INT_DATE_NEW="'"${INT_DATE}"'"
  echo " New date format is $INT_DATE_NEW"

  echo -e " Running Stored Procs & creating load ready files \n"
  while IFS=',' read schema procName tableName; do

	echo "use $schema;" > $script_dir/file_dyn
	echo "call $procName("${tableName}","${INT_DATE_NEW}");" >> $script_dir/file_dyn
	mysql -s < $script_dir/file_dyn

  done<$script_dir/proc_list_dyn
  rm  $script_dir/file_dyn

#Renameing all the .csv files thus created with Timestamp:

  echo -e "Renaming all the .csv file with Timestamp \n"   

  ls -1rt $inp_dir/*.csv|rev|awk -F"/" '{ print $1 }' |rev > $script_dir/filelist_dyn
  cat $script_dir/filelist_dyn
  while read fileName; do
	
	fileNameNew=`echo $fileName|awk -F ".csv" '{ print $1 }'`
	mv $inp_dir/$fileName $inp_dir/$fileNameNew'_'$date_var'.csv'

  done<$script_dir/filelist_dyn
  rm $script_dir/filelist_dyn


#Copy ReactivationFee_SERVICE_ASSET file from Extract Dir if present 

  mod_dt=`echo $INT_DATE|sed 's/-//g'`
  new_ext_dir=$mod_dt'_''PolycomExtracts'
  if stat --printf='' $ext_dir/$new_ext_dir/Polycom_ReactivationFee_SERVICE_ASSET* 2>/dev/null; then
	cp $ext_dir/$new_ext_dir/Polycom_ReactivationFee_SERVICE_ASSET* $inp_dir/
	echo -e "\n Copied ReactivationFee_SERVICE_ASSET File/s to boomi_load dir, to be sftp'd"
  else
	echo -e "\n No ReactivationFee_SERVICE_ASSET File/s to be copied"
  fi


#LFTP ing the csv files to the SFTP server:

  echo -e "\n LFTP ing the csv files to the SFTP server: \n"

  ls -1rt $inp_dir/*.csv|rev|awk -F"/" '{ print $1 }' |rev > $script_dir/filelist
  cd $inp_dir
  while read fileName; do

	echo -e " Moving file $fileName to SFTP server \n"
	lftp -e "cd /$lftp_tmp_loc; put $fileName; mv /$lftp_tmp_loc/$fileName /$lftp_act_loc/$fileName; bye" -u $user,$pass sftp://$server
	echo -e " File $fileName moved successfully to SFTP server \n"

  done<$script_dir/filelist
  rm $script_dir/filelist
  
#END
