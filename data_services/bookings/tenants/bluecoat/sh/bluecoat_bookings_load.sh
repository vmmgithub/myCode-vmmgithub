#!/bin/bash

invoked=$_

OPTIND=1

#
# Shell script to load data files into MySQL
# Splits large files into 1,000,000 line chunks
# and loads them into the TABLE corresponding to the file name
#
#source ${CONFIG_FILE}
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
SCHEMA=bluecoat_bookings
#
# Incremental load date (unusable date as default)
#
INC_DATE="3000-01-01"
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
while getopts ":u:p:s:l:i:h:?:" Option
do
	case $Option in
		u	) USERID=$OPTARG;;
		h	) MYSQLHOST=$OPTARG;;
		s	) SCHEMA=$OPTARG;;
		i	) INC_DATE=$OPTARG;;
		l	) IS_LOCAL="LOCAL";;
		?	) USAGE
			  exit 0;;
		*	) echo ""
			  echo "Unimplemented option chosen."
			  USAGE   # DEFAULT
	esac
done
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

LINE_END="'\n'"

mysql -u $USERID -h$MYSQLHOST $SCHEMA -e "truncate $SCHEMA.BluecoatBookingBulk; show warnings; show errors;"
mysql -u $USERID -h$MYSQLHOST $SCHEMA -e "truncate $SCHEMA.BluecoatBookingBulkOSC; show warnings; show errors;"

for data_file in $@; do

	# Must enter code to truncate bulk load tables


	echo $(date) Started processing "$data_file"
	#
	# Check for 0 length files
	#
	if [ ! -s $data_file ]; then
		echo "$(date) $data_file is 0 length file. Skipping..."
		continue
	fi
	#
	# Determine correct Line Terminator
	#
	if [[ "$(grep -l $'\r' $data_file)" == "$data_file" ]]
	then
		LINE_END="'\r\n'"
		echo "$(date) DOS text file with '\r\n'"
	else
		LINE_END="'\n'"
		echo "$(date) text file with '\n'"
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
		if [[ "$DATA_FILE" = *BLUECOAT_DISCOVERER_RENEWAL* ]]
		then
			if [[ $IG_NUM -eq 1 ]]
			then
				if [[ "`head -1 $SEGMENT | grep \"$BLUECOAT_DISCOVERER_RENEWAL_HEADER\" | wc -l`" == "0" ]]
				then
					echo "$(date) === ERROR File $DATA_FILE Does not have correct layout ==="
					source ${LOG_SQL_SH} "Loading Client File $DATA_FILE" 1 "$DATA_FILE does not have correct layout" 0
					exit 1
				fi
			fi
			DATA_LOAD_STRING="load data $IS_LOCAL infile '$SEGMENT' into table
				$SCHEMA.BluecoatBookingBulk
				character set latin1
				fields terminated by ',' optionally enclosed by '\"'
				lines terminated by $LINE_END ignore $IG_NUM lines (
			OrderNumber,
			OrderedDate,
			BookedDate,
			InvoiceNumber,
			InvoiceDate,
			EntryStatus,
			OrderType,
			OrderSource,
			PurchaseOrderNumber,
			Customer,
			CustomerNumber,
			EndUser,
			EndUserAccountNumber,
			Reseller,
			ResellerAccountNumber,
			LineNumber,
			LineType,
			LineStatus,
			PriceList,
			Nsp,
			Dan,
			PartNumber,
			ItemDescription,
			ProductModel,
			ServiceType,
			ServiceStartDate,
			ServiceEndDate,
			SiebelQuoteNumber,
			Quote,
			QuoteLine,
			QuoteSerialNumber,
			OrderedQuantity,
			SellingPrice,
			Ext\$Value,
			LicenseCount,
			SellingOrganization,
			BillToState,
			BillToCountry,
			EndUserState,
			EndUserCountry,
			Region
			);"

		elif [[ "$DATA_FILE" = *BLUECOAT_OSC* ]]
		then
			if [[ $IG_NUM -eq 1 ]]
			then
				if [[ "`head -1 $SEGMENT | grep \"$BLUECOAT_OSC_HEADER\" | wc -l`" == "0" ]]
				then
					echo "$(date) === ERROR File $DATA_FILE Does not have correct layout ==="
					source ${LOG_SQL_SH} "Loading Client File $DATA_FILE" 1 "$DATA_FILE does not have correct layout" 0
					exit 1
				fi
			fi
			DATA_LOAD_STRING="load data $IS_LOCAL infile '$SEGMENT' into table
				$SCHEMA.BluecoatBookingBulkOSC
				character set latin1
				fields terminated by ',' optionally enclosed by '\"'
				lines terminated by $LINE_END ignore $IG_NUM lines (
			InvoiceNumber,
			InvoiceDate,
			OrderBookedDate,
			SourceNumber,
			CustomerPoNumber,
			BillToCustomerName,
			EndCustomer,
			ResellerName,
			ServiceType,
			ItemNumber,
			ServiceName,
			Product,
			StartDate,
			EndDate,
			SerialNumber,
			QuantityInvoiced,
			UnitSellingPrice,
			ExtendedAmount,
			BillToState,
			BillToCountry,
			Region1,
			EndUserState,
			EndUserCountry,
			Region
			);"

		else
			echo $(date) ERROR "$DATA_FILE" did not match any existing table names.  Skipping...
			DATA_LOAD_STRING=""
		fi

		#
		# Time to load the table
		#
		#echo Length of DATA_LOAD_STRING ${#DATA_LOAD_STRING}
		if [ ${#DATA_LOAD_STRING} -gt 0 ]
		then
			#echo time mysql -u $USERID -p -h$MYSQLHOST $SCHEMA -e "$SQL_VARS $DATA_LOAD_STRING"
			mysql -u $USERID -h$MYSQLHOST $SCHEMA -e "$SQL_VARS $DATA_LOAD_STRING show warnings; show errors;"
			source ${LOG_SQL_SH} 'LoadIntoMySQL03' 1 "$(date) ===SUCCESSFULLY loaded $SEGMENT ===" $((`wc -l $SEGMENT | awk '{print $1}'`-$IG_NUM))
			PROCESSED=1
		else
			PROCESSED=0
		fi
		#
		# Remove the processed segment
		#
		rm -f $SEGMENT
		#
		# Reset the ignore lines to 0 after 1 segment
		#
		IG_NUM=0
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
