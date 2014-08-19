#!/bin/bash

SCHEMA=
FORCE_CREATE="false"
INDEX_EXISTS=

function usage() {
	echo "Usage: $0 -s <polycom|Polycom_UAT> -f (force creation of index)" 1>&2
	exit 1
}
####################################################################################
# Get the command line arg
####################################################################################
while getopts ":fs:" arg; do
	case "${arg}" in
		s) SCHEMA=${OPTARG} ;;
		f) FORCE_CREATE="true" ;;
		*) usage ;;
	esac
done
shift $((OPTIND-1))

if [[ "$SCHEMA" != "polycom" && "$SCHEMA" != "Polycom_UAT" ]]
then
	usage
fi


#
# Check if the index already exists
#
read INDEX_EXISTS <<< $(mysql -s mysql -e "select count(0) from mysql.innodb_index_stats where index_name = 'ix_win_match' and database_name = '$SCHEMA' and table_name='APP_OFFERS';")

if [[ "$INDEX_EXISTS" == "0" || "$FORCE_CREATE" == "true" ]]
then
mysql $SCHEMA -f -s -s -e "
use $SCHEMA;
drop INDEX ix_win_match on APP_OFFERS;
create INDEX ix_win_match on APP_OFFERS 
	(EXTENSIONS_TENANT_ASSETID_VALUE, EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE,
	EXTENSIONS_TENANT_ENTITLEID_VALUE);
"
fi


read INDEX_EXISTS <<< $(mysql -s mysql -e "select count(0) from mysql.innodb_index_stats where index_name = 'ix_subordinate' and database_name = '$SCHEMA' and table_name='APP_OPPORTUNITIES';")

if [[ "$INDEX_EXISTS" == "0" || "$FORCE_CREATE" == "true" ]]
then
mysql $SCHEMA -f -s -s -e "
use $SCHEMA;
drop INDEX ix_subordinate on APP_OPPORTUNITIES;
create INDEX ix_subordinate on APP_OPPORTUNITIES(_ID,ISSUBORDINATE);
"
fi


read INDEX_EXISTS <<< $(mysql -s mysql -e "select count(0) from mysql.innodb_index_stats where index_name = 'ix_agree_line' and database_name = 'polycom_data' and table_name='ENTITLEMENT_DAILY_ACTIVITY';")

if [[ "$INDEX_EXISTS" == "0" || "$FORCE_CREATE" == "true" ]]
then
mysql $SCHEMA -f -s -s -e "
use $SCHEMA;
drop INDEX ix_agree_line on polycom_data.ENTITLEMENT_DAILY_ACTIVITY;
create INDEX ix_agree_line on polycom_data.ENTITLEMENT_DAILY_ACTIVITY(ASSET_ID, PREV_ENTITLEMENT_ID,
	AGREE_LINE_SERVICE_PART_NUM);
"
fi
