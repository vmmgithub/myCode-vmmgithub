#!/bin/bash

#
# Shell script to load data files into MySQL
# Splits large files into 1000,000 line chunks
# and loads them into the TABLE corresponding to the file name
#

#
# User ID
#
USERID="dataadmin"
#
# MySQL Host
#
MYSQLHOST=localhost
#
# Schema to USE
#
SCHEMA=polycom_data
#
# Incremental load date (unusable date as default)
#
INC_DATE='3000-01-01'
#
# Are the data files to be loaded on the MySQL server or Locally located?
# Leave this blank if they are on MySQL server
#
IS_LOCAL="LOCAL"

function USAGE () {
	echo ""
	echo "USAGE: "
	echo "    load.sh [-?uipshl] <FileNames>"
	echo ""
	echo "OPTIONS:"
	echo "    -u  User id for mysql"
	echo "    -i  Incremental data date in 'yyyy-mm-dd' format"
	echo "    -s  Schema to use"
	echo "    -h  Mysql host"
	echo "    -l  if the data files are local on mysql client"
	echo "    -?  this usage information"
	echo ""
	echo "EXAMPLE:"
	echo "    load.sh -u userid -s polycom_stg Belgium*"
	echo ""
	exit $E_OPTERROR    # Exit and explain usage, if no argument(s) given.
}

#PROCESS ARGS
while getopts ":u:p:s:l:i:h:?" Option
do
	case $Option in
	    u    ) USERID=$OPTARG;;
	    h    ) MYSQLHOST=$OPTARG;;
	    s    ) SCHEMA=$OPTARG;;
	    i    ) INC_DATE=$OPTARG;;
	    l    ) IS_LOCAL="LOCAL";;
	    ?    ) USAGE
	           exit 0;;
	    *    ) echo ""
	           echo "Unimplemented option chosen."
	           USAGE   # DEFAULT
	esac
done
TMP_DIR="/tmp"
#
#  Decrements the argument pointer so it points to next argument.
#
shift $(($OPTIND - 1))

if [ -n "$1" ]
then
#
# The following 4 need to be set at database level
#
#innodb_buffer_pool_size=10G;
#innodb_flush_method=O_DIRECT;
#innodb_log_file_size=256M;
#innodb_doublewrite=0;
#
# PREPARE all optimization parameters
#
read -d '' SQL_VARS <<-"EO_SQL_VARS"
	set unique_checks = 0;
	set foreign_key_checks = 0;
	set sql_log_bin = 0;
	set NAMES 'utf8';
	set GLOBAL innodb_flush_log_at_trx_commit = 2;
EO_SQL_VARS

for data_file in $@; do
	echo $(date) Started processing "$data_file"
	#
	# Check for 0 length files
	#
	if [ ! -s $data_file ]; then
		echo "$(date) $data_file is 0 length file. Skipping..."
		continue
	fi
	echo "$(date) splitting $data_file... 1,000,000 line segments"
	PREFIX=chunk_${RANDOM}_${RANDOM}
	FNAME_NOPATH="${data_file##*/}"
	split -l 1000000 $data_file $TMP_DIR/$PREFIX$FNAME_NOPATH
	PROCESSED=0
	#
	# Set the ignore to 1 for the first iteration
	#
	IG_NUM=1
	#
	# Check for 0 length files
	#
	if ! ls $TMP_DIR/$PREFIX* &> /dev/null; then
		echo "$(date) $data_file is 0 length file. Skipping..."
		continue
	fi
	for SEGMENT in $TMP_DIR/$PREFIX*; do
		echo "$(date) On segment $SEGMENT"
		#
		# Convert to upper case
		#
		DATA_FILE=`echo ${FNAME_NOPATH} | tr 'a-z' 'A-Z'`
		#
		# Decide which table it is by the file name
		#
		if [[ "$DATA_FILE" = *DELIVERY_OF_DAILY_ORDERS_ACTIVITY* ]]
		then
			DATA_LOAD_STRING="load data $IS_LOCAL infile '$SEGMENT' into table
				$SCHEMA.DELIVERY_OF_DAILY_ORDERS_ACTIVITY 
				fields terminated by '|' optionally enclosed by '\"'
				lines terminated by '\r\n' ignore $IG_NUM lines (
			AGREE_PO,
			AGREE_SO,
			AGREE_ACCNT_NAME,
			AGREE_LINE_SERVICE_PART_NUM,
			AGREE_LINE_SERVICE_PRODUCT,
			AGREE_LINE_PRODUCT_GROUP,
			AGREE_SHIP_ACCNT_NAME,
			@t_INVOICE_DATE,
			INVOICE_NUM,
			SO_CURRENCY,
			@t_SO_DATE,
			@t_SO_EXT_NET_PRICE_USD,
			@t_SO_EXT_NET_PRICE,
			DOC_CURR_CODE,
			LOC_CURR_CODE,
			SS_IMPORT_DT
			) set SS_IMPORT_DT = '$INC_DATE',
			SO_EXT_NET_PRICE_USD=NULLIF(@t_SO_EXT_NET_PRICE_USD,''),
			SO_EXT_NET_PRICE=NULLIF(@t_SO_EXT_NET_PRICE,''),
			INVOICE_DATE=IF(LENGTH(@t_INVOICE_DATE) > 0,
				STR_TO_DATE(@t_INVOICE_DATE, '%m/%d/%Y %H:%i:%s'), NULL),
			SO_DATE=IF(LENGTH(@t_SO_DATE) > 0,
				STR_TO_DATE(@t_SO_DATE, '%m/%d/%Y %H:%i:%s'), NULL);"

		elif [[ "$DATA_FILE" = *ENTITLEMENT_DAILY_ACTIVITY* ]]
		then
			DATA_LOAD_STRING="load data $IS_LOCAL infile '$SEGMENT' into table
				polycom_data.ENTITLEMENT_DAILY_ACTIVITY_HISTORICAL
				fields terminated by '|' optionally enclosed by '\"'
				lines terminated by '\n' ignore $IG_NUM lines (
			AGREE_ID,
			AGREE_NAME,
			AGREE_NUM,
			AGREE_PO,
			AGREE_SALES_REGION,
			AGREE_SO,
			NO_OF_ASSET_ENTL_ON_LINE,
			AGREE_LINE_ID,
			AGREE_LINE_NUM,
			AGREE_LINE_PART_LIST,
			@t_AGREE_LINE_PART_NET,
			@t_AGREE_LINE_PART_NET_PER_ASSET,
			AGREE_LINE_QTY,
			AGREE_LINE_CURRENCY,
			AGREE_LINE_SERVICE_PART_NUM,
			AGREE_LINE_SERVICE_PRODUCT,
			ASSET_NUM,
			ASSET_ID,
			ASSET_PO_NUM,
			ASSET_SERIAL_NUM,
			ASSET_SERVICE_REGION,
			@t_ASSET_SHIP_DATE,
			ASSET_SO_NUM,
			ASSET_ADDRESS_1,
			ASSET_ADDRESS_2,
			ASSET_ADDRESS_ID,
			ASSET_CITY,
			ASSET_COUNTRY,
			ASSET_POSTAL,
			ASSET_STATE,
			ASSET_PART_NUM,
			ASSET_PRODUCT,
			ASSET_PRODUCT_DIVISION,
			ASSET_PRODUCT_GROUP,
			ASSET_PRODUCT_ID,
			ASSET_PRODUCT_LINE,
			@t_ENTL_CREATE_DATE,
			ENTL_DELIVERY_TYPE,
			@t_ENTL_END_DATE,
			ENTL_ID,
			ENTL_NAME,
			@t_ENTL_NET_PRICE_PER_ASSET,
			@t_ENTL_NET_PRICE_PER_ASSET_USD,
			ENTL_SERVICE_TYPE,
			@t_ENTL_ST_DATE,
			PREV_AGREE_ID,
			PREV_ENTL_END,
			PREV_ENTL_PO,
			SO_CURRENCY,
			@t_SO_DATE,
			@t_SO_EXT_NET_PRICE,
			@t_SO_EXT_NET_PRICE_USD,
			PREV_ENTL_SRVC_PART_NUM,
			AGREE_ACCNT_ID,
			AGREE_ACCNT_NAME,
			AGREE_ACCNT_GAN,
			AGREE_ACCNT_ADDRESS_1,
			AGREE_ACCNT_ADDRESS_2,
			AGREE_ACCNT_ADDRESS_ID,
			AGREE_ACCNT_CITY,
			PREV_ENTITLEMENT_ID,
			AGREE_ACCNT_COUNTRY,
			AGREE_ACCNT_POSTAL,
			AGREE_ACCNT_STATE,
			AGREE_ACCNT_CONTACT_EMAIL,
			AGREE_ACCNT_CONTACT_NAME,
			AGREE_ACCNT_CONTACT_PHONE,
			AGREE_ACCNT_PGS_CONTACT_EMAIL,
			AGREE_ACCNT_PGS_CONTACT_FIRST,
			AGREE_ACCNT_PGS_CONTACT_ID,
			AGREE_ACCNT_PGS_CONTACT_LAST,
			AGREE_ACCNT_PGS_CONTACT_PHONE,
			AGREE_END_CUST_ID,
			AGREE_END_CUST_NAME,
			AGREE_END_CUST_ADDRESS_1,
			AGREE_END_CUST_ADDRESS_2,
			AGREE_END_CUST_ADDRESS_ID,
			AGREE_END_CUST_CITY,
			AGREE_END_CUST_COUNTRY,
			AGREE_END_CUST_POSTAL,
			AGREE_END_CUST_STATE,
			AGREE_END_CUST_CONTACT_EMAIL,
			AGREE_END_CUST_CONTACT_NAME,
			AGREE_END_CUST_CONTACT_PHONE,
			AGREE_END_CUST_PGS_CONTACT_EMAIL,
			AGREE_END_CUST_PGS_CONTACT_FIRST,
			AGREE_END_CUST_PGS_CONTACT_ID,
			AGREE_END_CUST_PGS_CONTACT_LAST,
			AGREE_END_CUST_PGS_CONTACT_PHONE,
			AGREE_RES_ACCNT_NAME,
			AGREE_RES_ID,
			AGREE_RES_ADDRESS_1,
			AGREE_RES_ADDRESS_2,
			AGREE_RES_ADDRESS_ID,
			AGREE_RES_CITY,
			AGREE_RES_COUNTRY,
			AGREE_RES_POSTAL,
			AGREE_RES_STATE,
			AGREE_RES_CONTACT_EMAIL,
			AGREE_RES_CONTAC_NAME,
			AGREE_RES_CONTACT_PHONE,
			AGREE_RES_PGS_CONTACT_EMAIL,
			AGREE_RES_PGS_CONTACT_FIRST,
			AGREE_RES_PGS_CONTACT_ID,
			AGREE_RES_PGS_CONTACT_LAST,
			AGREE_RES_PGS_CONTACT_PHONE,
			AGREE_SHIP_ACCNT_NAME,
			AGREE_SHIP_ID,
			AGREE_SHIP_ADDRESS_1,
			AGREE_SHIP_ADDRESS_2,
			AGREE_SHIP_ADDRESS_ID,
			AGREE_SHIP_CITY,
			AGREE_SHIP_COUNTRY,
			AGREE_SHIP_POSTAL,
			AGREE_SHIP_STATE,
			AGREE_SHIP_CONTACT_EMAIL,
			AGREE_SHIP_CONTACT_NAME,
			AGREE_SHIP_CONTACT_PHONE,
			AGREE_SHIP_PGS_CONTACT_EMAIL,
			AGREE_SHIP_PGS_CONTACT_FIRST,
			AGREE_SHIP_PGS_CONTACT_ID,
			AGREE_SHIP_PGS_CONTACT_LAST,
			AGREE_SHIP_PGS_CONTACT_PHONE,
			ASSET_OWNER_ACCNT_NAME,
			ASSET_OWNER_ID,
			ASSET_OWNER_ADDRESS_1,
			ASSET_OWNER_ADDRESS_2,
			ASSET_OWNER_ADDRESS_ID,
			ASSET_OWNER_CITY,
			ASSET_OWNER_COUNTRY,
			ASSET_OWNER_POSTAL,
			ASSET_OWNER_STATE,
			ASSET_CONTACT_EMAIL,
			ASSET_CONTACT_FIRST,
			ASSET_CONTACT_ID,
			ASSET_CONTACT_LAST,
			ASSET_CONTACT_PHONE,
			SS_IMPORT_DT
			) set SS_IMPORT_DT = '$INC_DATE',
			AGREE_LINE_PART_NET=NULLIF(@t_AGREE_LINE_PART_NET,''),
			AGREE_LINE_PART_NET_PER_ASSET=NULLIF(@t_AGREE_LINE_PART_NET_PER_ASSET,''),
			ENTL_NET_PRICE_PER_ASSET=NULLIF(@t_ENTL_NET_PRICE_PER_ASSET,''),
			ENTL_NET_PRICE_PER_ASSET_USD=NULLIF(@t_ENTL_NET_PRICE_PER_ASSET_USD,''),
			SO_EXT_NET_PRICE=NULLIF(@t_SO_EXT_NET_PRICE,''),
			SO_EXT_NET_PRICE_USD=NULLIF(@t_SO_EXT_NET_PRICE_USD,''),
			ASSET_SHIP_DATE=IF(LENGTH(@t_ASSET_SHIP_DATE) > 0,
				STR_TO_DATE(@t_ASSET_SHIP_DATE, '%m/%d/%Y %H:%i:%s'), NULL),
			ENTL_CREATE_DATE=IF(LENGTH(@t_ENTL_CREATE_DATE) > 0,
				STR_TO_DATE(@t_ENTL_CREATE_DATE, '%m/%d/%Y %H:%i:%s'), NULL),
			ENTL_END_DATE=IF(LENGTH(@t_ENTL_END_DATE) > 0,
				STR_TO_DATE(@t_ENTL_END_DATE, '%m/%d/%Y %H:%i:%s'), NULL),
			ENTL_ST_DATE=IF(LENGTH(@t_ENTL_ST_DATE) > 0,
				STR_TO_DATE(@t_ENTL_ST_DATE, '%m/%d/%Y %H:%i:%s'), NULL),
			SO_DATE=IF(LENGTH(@t_SO_DATE) > 0,
				STR_TO_DATE(@t_SO_DATE, '%m/%d/%Y %H:%i:%s'), NULL);"

		elif [[ "$DATA_FILE" = *WEEKLY_DISASSOCIATED_ASSETS* ]]
		then
			DATA_LOAD_STRING="load data $IS_LOCAL infile '$SEGMENT' into table
				$SCHEMA.WEEKLY_DISASSOCIATED_ASSETS
				fields terminated by '|' optionally enclosed by '\"'
				lines terminated by '\r\n' ignore $IG_NUM lines (
			INTEGRATION_ID,
			ASSET,
			ASSET_NUM,
			SERIAL_NUM,
			ENTITLEMENT,
			AGREEMENT,
			PRODUCT,
			AGREE_LINE_SERVICE_PART_NUM,
			OPERATION_CD,
			@t_ASSET_DISASSOCIATION_DT,
			PROD_INT_ID,
			AGREEMENT_ITEM,
			AGREEMENT_VALID_FLG,
			AGREEMENT_ACCOUNT,
			ASSET_OWNER_ACCOUNT,
			@t_ENTITLEMENT_START_DT,
			@t_ENTITLEMENT_END_DT,
			@t_AGREE_START_DT,
			@t_AGREE_END_DT,
			SS_IMPORT_DT
			) set SS_IMPORT_DT = '$INC_DATE',
			ASSET_DISASSOCIATION_DT=IF(LENGTH(@t_ASSET_DISASSOCIATION_DT) > 0,
				STR_TO_DATE(@t_ASSET_DISASSOCIATION_DT, '%m/%d/%Y %H:%i:%s'), NULL),
			ENTITLEMENT_START_DT=IF(LENGTH(@t_ENTITLEMENT_START_DT) > 0,
				STR_TO_DATE(@t_ENTITLEMENT_START_DT, '%m/%d/%Y %H:%i:%s'), NULL),
			ENTITLEMENT_END_DT=IF(LENGTH(@t_ENTITLEMENT_END_DT) > 0,
				STR_TO_DATE(@t_ENTITLEMENT_END_DT, '%m/%d/%Y %H:%i:%s'), NULL),
			AGREE_START_DT=IF(LENGTH(@t_AGREE_START_DT) > 0,
				STR_TO_DATE(@t_AGREE_START_DT, '%m/%d/%Y %H:%i:%s'), NULL),
			AGREE_END_DT=IF(LENGTH(@t_AGREE_END_DT) > 0,
				STR_TO_DATE(@t_AGREE_END_DT, '%m/%d/%Y %H:%i:%s'), NULL);"

		elif [[ "$DATA_FILE" = *WEEKLY_UNINSTALL_ASSETS* ]]
		then
			DATA_LOAD_STRING="load data $IS_LOCAL infile '$SEGMENT' into table
				$SCHEMA.WEEKLY_UNINSTALL_ASSETS
				fields terminated by '|' optionally enclosed by '\"'
				lines terminated by '\r\n' ignore $IG_NUM lines (
			@t_ASSET_UNINSTALL_DATE,
			ASSET_NUMBER,
			ASSET_ID,
			@t_ASSET_SHIP_DATE,
			ASSET_SERIAL_NUM,
			AGREE_ID,
			AGREE_LINE_ID,
			AGREE_LINE_SERVICE_PART_NUM,
			ENTL_ID,
			@t_ENTL_END_DATE,
			SS_IMPORT_DT
			) set SS_IMPORT_DT = '$INC_DATE',
			ASSET_UNINSTALL_DATE=IF(LENGTH(@t_ASSET_UNINSTALL_DATE) > 0,
				STR_TO_DATE(@t_ASSET_UNINSTALL_DATE, '%m/%d/%Y %H:%i:%s'), NULL),
			ASSET_SHIP_DATE=IF(LENGTH(@t_ASSET_SHIP_DATE) > 0,
				STR_TO_DATE(@t_ASSET_SHIP_DATE, '%m/%d/%Y %H:%i:%s'), NULL),
			ENTL_END_DATE=IF(LENGTH(@t_ENTL_END_DATE) > 0,
				STR_TO_DATE(@t_ENTL_END_DATE, '%m/%d/%Y %H:%i:%s'), NULL);"

		else
			echo $(date) ERROR "$DATA_FILE" did not match any existing table names.  Skipping...
			DATA_LOAD_STRING=""
		fi
		#
		# Reset the ignore lines to 0 after 1 segment
		#
		IG_NUM=0

		#
		# Time to load the table
		#
		#echo Length of DATA_LOAD_STRING ${#DATA_LOAD_STRING}
		if [ ${#DATA_LOAD_STRING} -gt 0 ]
		then
			#echo time mysql -u $USERID -p -h$MYSQLHOST $SCHEMA -e "$SQL_VARS $DATA_LOAD_STRING"
			mysql -u $USERID -h$MYSQLHOST $SCHEMA -e "$SQL_VARS $DATA_LOAD_STRING show warnings; show errors;"
			PROCESSED=1
		else
			PROCESSED=0
		fi
		#
		# Remove the processed segment
		#
		rm -f $SEGMENT
	done
	#echo Processed value is $PROCESSED
	if [ $PROCESSED -eq 1 ]
	then
		#touch $data_file.processed
		echo $(date) Completed processing "$data_file"
	fi
done
else
	echo "Usage $0 <file-to-load>";
fi
