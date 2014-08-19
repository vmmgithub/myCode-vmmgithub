#!/bin/bash

########################################################################################################################################################################################
########################################################################################################################################################################################
# CRON Entry Point to refresh mongo data from Renew production to MySQL
########################################################################################################################################################################################
########################################################################################################################################################################################

COMMON_SCRIPT_PATH=/data/software/Implementations/data_services/scripts/api_scripts/common/sh
CUSTOM_PATH=/data/software/Implementations/data_services/sql

while read tenant
do
	echo -e "\n\n Starting download of files for tenant ${tenant} from S3 server & creation of MySQL ready files"
	cd ${COMMON_SCRIPT_PATH}

	./restoreProdMongoExtractToSQL.sh -t ${tenant} -o all -c ${CUSTOM_PATH}/maps/downloadAllOpps.${tenant}.custom.map -s "${tenant}" -q ${CUSTOM_PATH}/tenant_setup/setup.${tenant}.sql

	if [[ $? -ne 0 ]]; then
		o -e "\n\n Error in refreshing MySQL data for tenant $tenant \n\n"
	else
		echo "Completed refreshing MySQL data for tenant $tenant !!!"
	fi
done < ${COMMON_SCRIPT_PATH}/tenant_list_file
