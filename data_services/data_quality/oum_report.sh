#!/bin/bash

COMMON_SCRIPT_PATH=/data/software/Implementations/data_services/scripts/api_scripts/common/sh

while read tenant
do
	mysql -s -f --skip-column-names ${tenant} < ./oum_query.sql | sed -e "s/^/${tenant}\t/"
done < ${COMMON_SCRIPT_PATH}/tenant_list_file
