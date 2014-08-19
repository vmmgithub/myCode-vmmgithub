#!/bin/bash

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${BASE_DIR}/polycom_resolve_common.sql.sh
#
# 'Unmatched' exception report only for the processing date
#
UNINSTALL_EXCEPTION_DATE_CLAUSE=""
DISASSOCIATE_EXCEPTION_DATE_CLAUSE=""

if [[ "$REPORT_ALL_EXCEPTIONS" == "1" ]]
then
	echo "$(date) Exceptions for all 'Not Processed' records will be reported"
else
	echo "$(date) Exceptions for 'Not Processed' records of '$PROCESS_DATE' will be reported"
	UNINSTALL_EXCEPTION_DATE_CLAUSE="AND wua.SS_IMPORT_DT='$PROCESS_DATE'"
	DISASSOCIATE_EXCEPTION_DATE_CLAUSE="AND wda.SS_IMPORT_DT='$PROCESS_DATE'"
fi

#
# -s -s (two of them) are purposefully used
#
mysql $SCHEMA -s -s -e "
/*
** 1. JOIN offers to quotes to opportunities with sales state not in \"closed sale\" \"house account\" \"no service\"
** 2. Collect all OFFERS from the opportunities
** 3. For each opportunity if all offers matched
** 	a. Calculate Result Reason
** 	b. House Account Opportunity
** 4. For each opportunity with some offers matched
** 	a. SPLIT OPPORTUNITY
** 	b. Calculate Result Reason
** 	c. House Account Opportunity
*/
/*
** 1. Create temporary tables so that it is easier to process
** 2. For each use case create a separate script
*/

SET tmp_table_size=64000000;
SET max_heap_table_size=64000000;
SET group_concat_max_len=64000;

SELECT NOW(), ' Polycom resolve as loss script started' from DUAL;
CREATE DATABASE IF NOT EXISTS polycom_tmp default character set utf8;
DROP TABLE IF EXISTS polycom_tmp.split_opportunity_tmp;
/*
**=====<step 1>=====
**	1.0 Matching for WEEKLY_DISASSOCIATED_ASSETS
*/
DROP TABLE IF EXISTS polycom_tmp.weekly_disassociated_assets_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.weekly_disassociated_assets_tmp (
	TMP_ID			INTEGER NOT NULL,
	MATCH_STATUS		varchar(50),
	OFFER_ID		varchar(50),
	EXCEPTION_ID		INTEGER DEFAULT 0,
	PROCESS_DT		TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	KEY ix_tmp_id (TMP_ID)
);
DROP TABLE IF EXISTS polycom_tmp.t_id;
CREATE TABLE IF NOT EXISTS polycom_tmp.t_id (
	TMP_ID			INTEGER NOT NULL
) ENGINE=MEMORY;

SELECT NOW(), ' Executing Step 1.1' from DUAL;
/*
**	1.1 MATCHED (WEEKLY_DISASSOCIATED_ASSETS)
**	Appendix 2
*/
SET autocommit=0;
INSERT INTO polycom_tmp.weekly_disassociated_assets_tmp (
	TMP_ID,
	MATCH_STATUS,
	OFFER_ID
)
SELECT DISTINCT
	wd.TMP_ID,
	'Matched',
	t_offers.OFFID
FROM
	polycom_data.WEEKLY_DISASSOCIATED_ASSETS wd
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON wd.ASSET = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND wd.SERIAL_NUM = offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE
	AND wd.ENTITLEMENT_END_DT = DATE(offers.TARGETDATE)
	AND wd.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
	AND wd.ENTITLEMENT = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OFFID = offers._ID
WHERE
	wd.SS_STATUS = 'Not Processed'
;
INSERT INTO polycom_tmp.t_id
SELECT DISTINCT
	TMP_ID
FROM
	polycom_tmp.weekly_disassociated_assets_tmp;
commit;

SELECT NOW(), ' Executing Step 1.2' from DUAL;
/*
**	1.2 NOT MATCHED (WEEKLY_DISASSOCIATED_ASSETS)
**	Appendix 2
*/
SET autocommit=0;
INSERT INTO polycom_tmp.weekly_disassociated_assets_tmp (
	TMP_ID,
	MATCH_STATUS,
	OFFER_ID,
	EXCEPTION_ID
)
SELECT DISTINCT
	wd.TMP_ID,
	'Not Matched',
	NULL,
	1
FROM
	polycom_data.WEEKLY_DISASSOCIATED_ASSETS wd
LEFT OUTER JOIN polycom_tmp.t_id tt
	ON tt.TMP_ID = wd.TMP_ID
WHERE
	wd.SS_STATUS = 'Not Processed'
	AND tt.TMP_ID IS NULL;
commit;

truncate polycom_tmp.t_id;

/*
**	1.2.0 Set the exception for those records without ASSET_DISASSOCIATION_DT
*/
SET autocommit=0;
INSERT INTO polycom_tmp.t_id
SELECT
	TMP_ID
FROM
	polycom_tmp.weekly_disassociated_assets_tmp wdat
WHERE
	wdat.MATCH_STATUS = 'Matched'
GROUP BY
	TMP_ID
HAVING
	COUNT(TMP_ID) > 1;

UPDATE	polycom_tmp.weekly_disassociated_assets_tmp wdat
INNER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wd
	ON wdat.TMP_ID = wd.TMP_ID
SET
	wdat.EXCEPTION_ID = 3
WHERE
	wd.ASSET_DISASSOCIATION_DT IS NULL;

/*
**	1.2.1 Set the exception for those records with multiple offers matched
*/
UPDATE	polycom_tmp.weekly_disassociated_assets_tmp wdat
INNER JOIN polycom_tmp.t_id tt
	ON tt.TMP_ID = wdat.TMP_ID
SET
	wdat.EXCEPTION_ID = 4; /* Multiple offers matched */

/*
**	1.2.2 Mark the exception records so that they don't get processed.
*/
UPDATE	polycom_tmp.weekly_disassociated_assets_tmp wdat
SET
	wdat.MATCH_STATUS = 'Exception'
WHERE
	wdat.EXCEPTION_ID > 1;
commit;
/*
**=====</step 1>=====
*/

/*
**=====<step 2>=====
**	2.0 Matching for WEEKLY_UNINSTALL_ASSETS
*/
DROP TABLE IF EXISTS polycom_tmp.weekly_uninstall_assets_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.weekly_uninstall_assets_tmp (
	TMP_ID			INTEGER NOT NULL,
	MATCH_STATUS		varchar(50),
	OFFER_ID		varchar(50),
	EXCEPTION_ID		INTEGER DEFAULT 0,
	PROCESS_DT		TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	KEY ix_tmp_id (TMP_ID)
);

truncate polycom_tmp.t_id;

SELECT NOW(), ' Executing Step 2.1' from DUAL;
/*
**	2.1 MATCHED (WEEKLY_UNINSTALL_ASSETS)
**	Appendix 2
*/
SET autocommit=0;
INSERT INTO polycom_tmp.weekly_uninstall_assets_tmp (
	TMP_ID,
	MATCH_STATUS,
	OFFER_ID
)
SELECT DISTINCT
	wua.TMP_ID,
	'Matched',
	t_offers.OFFID
FROM
	polycom_data.WEEKLY_UNINSTALL_ASSETS wua
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON wua.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND wua.ASSET_SERIAL_NUM = offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE
	AND wua.ENTL_END_DATE = DATE(offers.TARGETDATE)
	AND wua.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OFFID = offers._ID
WHERE
	wua.SS_STATUS = 'Not Processed'
;
commit;

INSERT INTO polycom_tmp.t_id
SELECT DISTINCT
	TMP_ID
FROM
	polycom_tmp.weekly_uninstall_assets_tmp;

SELECT NOW(), ' Executing Step 2.1' from DUAL;
/*
**	2.1 NOT MATCHED (WEEKLY_UNINSTALL_ASSETS)
**	Appendix 2
*/
SET autocommit=0;
INSERT INTO polycom_tmp.weekly_uninstall_assets_tmp (
	TMP_ID,
	MATCH_STATUS,
	OFFER_ID,
	EXCEPTION_ID
)
SELECT DISTINCT
	wua.TMP_ID,
	'Not Matched',
	NULL,
	1
FROM
	polycom_data.WEEKLY_UNINSTALL_ASSETS wua
LEFT OUTER JOIN polycom_tmp.t_id tt
	ON wua.TMP_ID = tt.TMP_ID
WHERE
	wua.SS_STATUS = 'Not Processed'
	AND tt.TMP_ID IS NULL;
commit;

truncate polycom_tmp.t_id;
/*
**	2.2.1 Set the exception for those records without ASSET_UNINSTALL_DT
*/
SET autocommit=0;
INSERT INTO polycom_tmp.t_id
SELECT
	TMP_ID
FROM
	polycom_tmp.weekly_uninstall_assets_tmp wuat
WHERE
	wuat.MATCH_STATUS = 'Matched'
GROUP BY
	TMP_ID
HAVING
	COUNT(TMP_ID) > 1;

UPDATE	polycom_tmp.weekly_uninstall_assets_tmp wuat
INNER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON wuat.TMP_ID = wua.TMP_ID
SET
	wuat.EXCEPTION_ID = 3
WHERE
	wua.ASSET_UNINSTALL_DATE IS NULL;
/*
**	1.2.1 Set the exception for those records with multiple offers matched
*/
UPDATE	polycom_tmp.weekly_uninstall_assets_tmp wuat
INNER JOIN polycom_tmp.t_id tt
	ON tt.TMP_ID = wuat.TMP_ID
SET
	wuat.EXCEPTION_ID = 4; /* Multiple offers matched */

/*
**	1.2.2 Mark the exception records so that they don't get processed.
*/
UPDATE	polycom_tmp.weekly_uninstall_assets_tmp wuat
SET
	wuat.MATCH_STATUS = 'Exception'
WHERE
	wuat.EXCEPTION_ID > 1;
commit;
/*
**=====</step 2>=====
*/

/*
**=====<step 3>=====
**	3.0 Process Matched Offers (WEEKLY_UNINSTALL_ASSETS)
*/

DROP TABLE IF EXISTS polycom_tmp.opp_uninstall_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.opp_uninstall_tmp (
	OPP_ID			VARCHAR(50),
	MATCH_STATUS		VARCHAR(50),
	RESOLUTION_DATE		DATE,
	RESOLUTION_REASON	VARCHAR(50),
	IS_REOPENED		BOOLEAN DEFAULT FALSE,
	KEY ix_opp_id_a_match_status (OPP_ID, MATCH_STATUS),
	KEY ix_match_status (MATCH_STATUS)
);

SELECT NOW(), ' Executing Step 3.1' from DUAL;
/*
**	3.1.1 Find out opportunies with 'noService' that should be re-opened
**	Appendix 9
*/
SELECT '_id(string)'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	opp._ID
FROM
	polycom_tmp.weekly_uninstall_assets_tmp wuat
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON wuat.MATCH_STATUS = 'Matched'
	AND t_offers.OFFID = wuat.OFFER_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME = 'noService'
	AND opp._ID IS NOT NULL

INTO OUTFILE '${EX_DIR}/polycom_uninstall_resolve_as_loss_reopen_opportunity_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	3.1.2 Identify records that match 'closedSale' and 'houseAccount' and mark them for exception
*/
SET autocommit=0;
/** <New> **/
UPDATE
	polycom_tmp.weekly_uninstall_assets_tmp wuat
INNER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON wuat.TMP_ID = wua.TMP_ID
	AND wuat.MATCH_STATUS = 'Not Matched'
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON wua.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND wua.ASSET_SERIAL_NUM = offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE
	AND wua.ENTL_END_DATE = DATE(offers.TARGETDATE)
	AND wua.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
LEFT OUTER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OFFID = offers._ID
LEFT OUTER JOIN ${SCHEMA}.T_OPP_QUOTES_OFFERS t_oqo
	ON offers._ID = t_oqo.OFFID
SET
	wuat.EXCEPTION_ID = 2,
	wuat.MATCH_STATUS = 'Exception'
WHERE
	wua.SS_STATUS = 'Not Processed'
	AND offers._ID is not null
	AND t_offers.OFFID IS NULL
	AND t_oqo.FLOWSTATE in ('closedSale', 'houseAccount')
;
/** </New> **/

UPDATE
	polycom_tmp.weekly_uninstall_assets_tmp wuat
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON wuat.MATCH_STATUS = 'Matched'
	AND t_offers.OFFID = wuat.OFFER_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
SET
	wuat.EXCEPTION_ID = 2
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME IN ('closedSale', 'houseAccount');
commit;
/*
**	3.1.3 Find out all matching opportunities (WEEKLY_UNINSTALL_ASSETS)
*/
SET autocommit=0;
INSERT INTO polycom_tmp.opp_uninstall_tmp (
	OPP_ID,
	MATCH_STATUS
)
SELECT DISTINCT
	opp._ID,
	NULL
FROM
	polycom_tmp.weekly_uninstall_assets_tmp wuat
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON wuat.MATCH_STATUS = 'Matched'
	AND t_offers.OFFID = wuat.OFFER_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME NOT IN ('closedSale', 'houseAccount');
/*
**	3.1.4 Mark the reopened opportunities (WEEKLY_UNINSTALL_ASSETS)
*/
UPDATE	polycom_tmp.opp_uninstall_tmp ou
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = ou.OPP_ID
SET
	ou.IS_REOPENED = true
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME = 'noService';
commit;


SELECT NOW(), ' Executing Step 3.2' from DUAL;
/*
**	3.2 Find out the partially matching opportunities (WEEKLY_UNINSTALL_ASSETS)
*/
DROP TABLE IF EXISTS polycom_tmp.opp_id_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.opp_id_tmp (
	OPP_ID	varchar(50),
	KEY ix_opp (OPP_ID)
) ENGINE=Memory DEFAULT character set utf8;

SET autocommit=0;
INSERT INTO polycom_tmp.opp_id_tmp
SELECT DISTINCT
	opp_u.OPP_ID
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wua.AGREE_LINE_SERVICE_PART_NUM
	AND wua.SS_STATUS = 'Not Processed'
WHERE
	wua.TMP_ID IS NULL;
commit;


SET autocommit=0;
UPDATE polycom_tmp.opp_uninstall_tmp ou
INNER JOIN polycom_tmp.opp_id_tmp oit
	ON ou.OPP_ID = oit.OPP_ID
SET
	ou.MATCH_STATUS = 'Partial Match';
commit;
TRUNCATE polycom_tmp.opp_id_tmp;

SELECT NOW(), ' Executing Step 3.3' from DUAL;
/*
**	3.3 Find out the fully matching opportunities (WEEKLY_UNINSTALL_ASSETS)
*/
SET autocommit=0;
INSERT INTO polycom_tmp.opp_id_tmp
SELECT DISTINCT
	opp_u.OPP_ID
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wua.AGREE_LINE_SERVICE_PART_NUM
WHERE
	wua.SS_STATUS = 'Not Processed'
	AND opp_u.MATCH_STATUS IS NULL;
commit;

SET autocommit=0;
UPDATE polycom_tmp.opp_uninstall_tmp ou
INNER JOIN polycom_tmp.opp_id_tmp oit
	ON ou.OPP_ID = oit.OPP_ID
	AND ou.MATCH_STATUS IS NULL
SET
	ou.MATCH_STATUS = 'Full Match';
commit;
TRUNCATE polycom_tmp.opp_id_tmp;

SELECT NOW(), ' Executing Step 3.4.0' from DUAL;
/*
**	3.4.0 Produce the \"IsExcluded\" Scrub for matched offers (WEEKLY_UNINSTALL_ASSETS)
**	Appendix 4
*/
SELECT
	'_id(string)',
	'IsExcluded(boolean)'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	offers._ID,
	'false'
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wua.AGREE_LINE_SERVICE_PART_NUM
WHERE
	wua.SS_STATUS = 'Not Processed'
	AND opp_u.MATCH_STATUS = 'Full Match'
	AND offers._ID IS NOT NULL
	AND offers.ISEXCLUDED = 'true'

INTO OUTFILE '${EX_DIR}/polycom_uninstall_resolve_as_loss_full_match_update_excluded_offers_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
SELECT NOW(), ' Executing Step 3.4.1' from DUAL;
/*
**	3.4.1 Produce the \"resolveAsLoss\" Scrub (WEEKLY_UNINSTALL_ASSETS)
**	Appendix 4
*/
SELECT
	'Name', /* '_id(string)', */
	'Reason', /* 'resultReason(string)' */
	'LossDate' /* 'resolutionDate(date)', */
FROM
	DUAL
UNION ALL
SELECT
	COALESCE(opp_u.OPP_ID, ''),
	'haPRT', -- 'HA – Product Return - PRT'
	COALESCE(MIN(wua.ASSET_UNINSTALL_DATE), '')
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wua.AGREE_LINE_SERVICE_PART_NUM
WHERE
	opp_u.MATCH_STATUS = 'Full Match'
	AND wua.SS_STATUS = 'Not Processed'
GROUP BY
	opp_u.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_uninstall_resolve_as_loss_full_match_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 3.5.0' from DUAL;
/*
**	3.5.0 Produce the the 'IsExcluded' scrub for \"partial matched\" portion of split opportunity offers (WEEKLY_UNINSTALL_ASSETS)
**	Appendix 4
*/
SELECT
	'_id(string)',
	'IsExcluded(boolean)'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	offers._ID,
	'false'
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_BASE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wua.AGREE_LINE_SERVICE_PART_NUM
WHERE
	opp_u.MATCH_STATUS = 'Partial Match'
	AND offers._ID IS NOT NULL
	AND offers.ISEXCLUDED = 'true'
	AND wua.SS_STATUS = 'Not Processed'

INTO OUTFILE '${EX_DIR}/polycom_uninstall_resolve_as_loss_partial_match_update_excluded_offers_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
SELECT NOW(), ' Executing Step 3.5.1' from DUAL;
/*
**	3.5.1 Produce the the scrub for \"partial matched\" portion of split opportunity (WEEKLY_UNINSTALL_ASSETS)
**	Appendix 4
*/
SELECT
	'Name', /* '_id(string)', */
	'Reason', /* 'resultReason(string)' */
	'LossDate' /* 'resolutionDate(date)', */
FROM
	DUAL
UNION ALL
SELECT
	opp_u.OPP_ID,
	'haPRT', -- 'HA – Product Return - PRT'
	COALESCE(MIN(wua.ASSET_UNINSTALL_DATE), '')
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
WHERE
	opp_u.MATCH_STATUS = 'Partial Match'
	AND wua.SS_STATUS = 'Not Processed'
	AND opp_u.OPP_ID IS NOT NULL
GROUP BY
	opp_u.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_uninstall_resolve_as_loss_partial_match_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 3.6' from DUAL;
/*
**	3.6 Produce the \"SPLIT Opportunity\" unmatched portion (WEEKLY_UNINSTALL_ASSETS)
**	Appendix 3
*/
SELECT
	'Opportunity',
	'OfferIds'
FROM
	DUAL
UNION ALL
SELECT
	opp_u.OPP_ID,
	group_concat(offers._ID)
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_BASE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wua.AGREE_LINE_SERVICE_PART_NUM
	AND wua.SS_STATUS = 'Not Processed'
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = opp_u.OPP_ID
	AND opp.ISSUBORDINATE <> 'true'
WHERE
	opp_u.MATCH_STATUS = 'Partial Match'
	AND wua.TMP_ID is NULL
	AND opp_u.OPP_ID IS NOT NULL
	AND offers._ID IS NOT NULL
GROUP BY
	opp_u.OPP_ID
INTO OUTFILE '${EX_DIR}/polycom_uninstall_split_opportunity_unmatched_offers_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 3.7' from DUAL;
/*
**	3.7 Recalculate Opportunity for partial matched portion of split opportunity (WEEKLY_UNINSTALL_ASSETS)
**	Appendix 8
*/
SELECT
	'_id(string)',
	'amount.amount(numeric)',
	'targetAmount.amount(numeric)',
	'extensions.tenant.priorRenewalAmountNotAnnualized.value.amount(numeric)',
	'extentions.tenant.listPrice.value.amount(numeric)',
	'targetDate(date)',
	'earliestNewStartDate(date)',
	'latestNewEndDate(date)'
FROM
	DUAL
UNION ALL
SELECT
	opp_u.OPP_ID,
	COALESCE(SUM(offers.AMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.TARGETAMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_PRIORRENEWALAMOUNTNOTANNUALIZED_VALUE_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_LISTPRICE_AMOUNT), ''),
	COALESCE(MIN(offers.TARGETDATE), ''),
	COALESCE(MIN(offers.STARTDATE), ''),
	COALESCE(MAX(offers.ENDDATE), '')
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wua.AGREE_LINE_SERVICE_PART_NUM
WHERE
	opp_u.MATCH_STATUS in ('Matched', 'Partial Match')
	AND wua.SS_STATUS = 'Not Processed'
	AND opp_u.OPP_ID IS NOT NULL
GROUP BY
	opp_u.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_uninstall_resolve_as_loss_recalculate_opportunity_match_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 3.8' from DUAL;
/*
**	3.8 Produce the \"SPLIT Opportunity\" unmatched portion (WEEKLY_UNINSTALL_ASSETS)
**	Appendix 8
*/
SELECT
	'_id(string)',
	'amount.amount(numeric)',
	'targetAmount.amount(numeric)',
	'extensions.tenant.priorRenewalAmountNotAnnualized.value.amount(numeric)',
	'extentions.tenant.listPrice.value.amount(numeric)',
	'targetDate(date)',
	'earliestNewStartDate(date)',
	'latestNewEndDate(date)'
FROM
	DUAL
UNION ALL
SELECT
	opp_u.OPP_ID,
	COALESCE(SUM(offers.AMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.TARGETAMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_PRIORRENEWALAMOUNTNOTANNUALIZED_VALUE_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_LISTPRICE_AMOUNT), ''),
	COALESCE(MIN(offers.TARGETDATE), ''),
	COALESCE(MIN(offers.STARTDATE), ''),
	COALESCE(MAX(offers.ENDDATE), '')
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_BASE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wua.AGREE_LINE_SERVICE_PART_NUM
	AND wua.SS_STATUS = 'Not Processed'
WHERE
	opp_u.MATCH_STATUS = 'Partial Match'
	AND wua.TMP_ID is NULL
	AND opp_u.OPP_ID IS NOT NULL
GROUP BY
	opp_u.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_uninstall_resolve_as_loss_recalculate_opportunity_partial_unmatched_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	3.8.1 \"Resolve Back to Original Sales Stage\" reopened \"split\" opportunity
**	Appendex 11
*/
SELECT
	'Name', /* '_id(string)', */
	'Reason', /* 'resultReason(string)' */
	'LossDate' /* 'resolutionDate(date)', */
FROM
	DUAL
UNION ALL
SELECT
	opp_u.OPP_ID,
	COALESCE(MIN(offers.RESULTREASON_NAME), ''),
	COALESCE(opp.RESOLUTIONDATE, '')
FROM
	polycom_tmp.opp_uninstall_tmp opp_u
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = opp_u.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_data.WEEKLY_UNINSTALL_ASSETS wua
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wua.ASSET_ID
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wua.ASSET_SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wua.ENTL_END_DATE
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wua.AGREE_LINE_SERVICE_PART_NUM
WHERE
	opp_u.MATCH_STATUS = 'Partial Match'
	AND opp_u.IS_REOPENED = true
	AND wua.SS_STATUS = 'Not Processed'
	AND wua.TMP_ID is NULL
	AND opp_u.OPP_ID IS NOT NULL
GROUP BY
	opp_u.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_uninstall_resolve_as_loss_reset_back_reopened_opportunities_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;


SELECT NOW(), ' Executing Step 3.9' from DUAL;
/*
**	3.9 Produce the exception report
**	Appendix 2
*/
SELECT
	'EXCEPTION',
	'ASSET_UNINSTALL_DATE',
	'ASSET_NUMBER',
	'ASSET_ID',
	'ASSET_SHIP_DATE',
	'ASSET_SERIAL_NUM',
	'AGREE_ID',
	'AGREE_LINE_ID',
	'AGREE_LINE_SERVICE_PART_NUM',
	'ENTL_ID',
	'ENTL_END_DATE'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	case
		/* when wuat.EXCEPTION_ID = 0 then 'Valid Data' */
		when wuat.EXCEPTION_ID = 1 then 'Failed to match'
		when wuat.EXCEPTION_ID = 2 then 'Matched - but already Closed Sale or House Account'
		when wuat.EXCEPTION_ID = 3 then 'Matched - but no Resolution Date'
		when wuat.EXCEPTION_ID = 4 then 'Multiple Offers Matched'
		else ''
	end as EXCEPTION_MSG,
	COALESCE(ASSET_UNINSTALL_DATE, ''),
	COALESCE(ASSET_NUMBER, ''),
	COALESCE(ASSET_ID, ''),
	COALESCE(ASSET_SHIP_DATE, ''),
	COALESCE(ASSET_SERIAL_NUM, ''),
	COALESCE(AGREE_ID, ''),
	COALESCE(AGREE_LINE_ID, ''),
	COALESCE(AGREE_LINE_SERVICE_PART_NUM, ''),
	COALESCE(ENTL_ID, ''),
	COALESCE(ENTL_END_DATE, '')
FROM
	polycom_data.WEEKLY_UNINSTALL_ASSETS wua
INNER JOIN polycom_tmp.weekly_uninstall_assets_tmp wuat
	ON wua.TMP_ID = wuat.TMP_ID
WHERE
	wuat.EXCEPTION_ID > 0
	${UNINSTALL_EXCEPTION_DATE_CLAUSE}

INTO OUTFILE '${EX_DIR}/polycom_uninstall_assets_exception_report_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
	
SELECT NOW(), ' Executing Step 3.10' from DUAL;
/*
**	3.10 Update the processed status of all 'Not Processed' WEEKLY_UNINSTALL_ASSETS
**	This will prevent us from reprocessing these records again.
*/
SET autocommit=0;
UPDATE polycom_data.WEEKLY_UNINSTALL_ASSETS wua
INNER JOIN polycom_tmp.weekly_uninstall_assets_tmp wuat
	ON wua.TMP_ID = wuat.TMP_ID
SET
	wua.SS_STATUS = 'Processed',
	wua.SS_PROCESSED_DT = NOW()
WHERE
	wuat.EXCEPTION_ID IN (0, 2)
	AND wua.SS_STATUS = 'Not Processed';
commit;

/*
**=====</step 3>=====
*/

/*
**=====<step 4>=====
**	4.0 Process Matched Offers (WEEKLY_DISASSOCIATED_ASSETS)
*/
DROP TABLE IF EXISTS polycom_tmp.opp_disassociate_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.opp_disassociate_tmp (
	OPP_ID			VARCHAR(50),
	MATCH_STATUS		VARCHAR(50),
	RESOLUTION_DATE		DATE,
	RESOLUTION_REASON	VARCHAR(50),
	IS_REOPENED		BOOLEAN DEFAULT FALSE,
	KEY ix_opp_id_a_match_status (OPP_ID, MATCH_STATUS),
	KEY ix_match_status (MATCH_STATUS)
);
SELECT NOW(), ' Executing Step 4.1' from DUAL;
/*
**	4.1.1 Find out opportunies with 'noService' that should be re-opened
**	Appendix 9
*/
SELECT '_id(string)'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	opp._ID
FROM
	polycom_tmp.weekly_disassociated_assets_tmp wdat
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON wdat.MATCH_STATUS = 'Matched'
	AND t_offers.OFFID = wdat.OFFER_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME = 'noService'
	AND opp._ID IS NOT NULL

INTO OUTFILE '${EX_DIR}/polycom_disassociate_resolve_as_loss_reopen_opportunity_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	4.1.2 Identify records that match 'closedSale' and 'houseAccount' and mark them for exception
*/
SET autocommit=0;
/** <New> **/
UPDATE
	polycom_tmp.weekly_disassociated_assets_tmp wdat
INNER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wd
	ON wdat.TMP_ID = wd.TMP_ID
	AND wdat.MATCH_STATUS = 'Not Matched'
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON wd.ASSET = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND wd.SERIAL_NUM = offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE
	AND wd.ENTITLEMENT_END_DT = DATE(offers.TARGETDATE)
	AND wd.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
	AND wd.ENTITLEMENT = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
LEFT OUTER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OFFID = offers._ID
LEFT OUTER JOIN ${SCHEMA}.T_OPP_QUOTES_OFFERS t_oqo
	ON offers._ID = t_oqo.OFFID
SET
	wdat.EXCEPTION_ID = 2,
	wdat.MATCH_STATUS = 'Exception'
WHERE
	wd.SS_STATUS = 'Not Processed'
	AND offers._ID is not null
	AND t_offers.OFFID IS NULL
	AND t_oqo.FLOWSTATE in ('closedSale', 'houseAccount')
;
/** </New> **/

UPDATE
	polycom_tmp.weekly_disassociated_assets_tmp wdat
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON wdat.MATCH_STATUS = 'Matched'
	AND t_offers.OFFID = wdat.OFFER_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
SET
	wdat.EXCEPTION_ID = 2
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME IN ('closedSale', 'houseAccount');
commit;

/*
**	4.1.3 Find out all matching opportunities (WEEKLY_DISASSOCIATED_ASSETS)
*/
SET autocommit=0;
INSERT INTO polycom_tmp.opp_disassociate_tmp (
	OPP_ID,
	MATCH_STATUS
)
SELECT DISTINCT
	opp._ID,
	NULL
FROM
	polycom_tmp.weekly_disassociated_assets_tmp wdat
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON wdat.MATCH_STATUS = 'Matched'
	AND t_offers.OFFID = wdat.OFFER_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME NOT IN ('closedSale', 'houseAccount');
/*
**	4.1.4 Mark the reopened opportunities (WEEKLY_DISASSOCIATED_ASSETS)
*/
UPDATE	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = opp_d.OPP_ID
SET
	opp_d.IS_REOPENED = true
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME = 'noService';
commit;


SELECT NOW(), ' Executing Step 4.2' from DUAL;
/*
**	4.2 Find out the partially matching opportunities (WEEKLY_DISASSOCIATED_ASSETS)
*/
INSERT INTO polycom_tmp.opp_id_tmp
SELECT DISTINCT
	opp_d.OPP_ID
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
	AND wda.SS_STATUS = 'Not Processed'
WHERE
	wda.TMP_ID IS NULL;
commit;

SET autocommit=0;
UPDATE polycom_tmp.opp_disassociate_tmp odt
INNER JOIN polycom_tmp.opp_id_tmp oit
	ON oit.OPP_ID = odt.OPP_ID
SET
	odt.MATCH_STATUS = 'Partial Match';
commit;
TRUNCATE polycom_tmp.opp_id_tmp;

SELECT NOW(), ' Executing Step 4.3' from DUAL;
/*
**	4.3 Find out the fully matching opportunities (WEEKLY_DISASSOCIATED_ASSETS)
*/
SET autocommit=0;
INSERT INTO polycom_tmp.opp_id_tmp
SELECT DISTINCT
	opp_d.OPP_ID
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
WHERE
	opp_d.MATCH_STATUS IS NULL
	AND wda.SS_STATUS = 'Not Processed';
commit;
SET autocommit=0;
UPDATE polycom_tmp.opp_disassociate_tmp odt
INNER JOIN polycom_tmp.opp_id_tmp oit
	ON odt.OPP_ID = oit.OPP_ID
	AND odt.MATCH_STATUS IS NULL
SET
	odt.MATCH_STATUS = 'Full Match';
commit;

SELECT NOW(), ' Executing Step 4.4.0' from DUAL;
/*
**	4.4.0 Produce the \"IsExcluded\" Scrub for matched offers (WEEKLY_DISASSOCIATED_ASSETS)
**	Appendix 4
*/
SELECT
	'_id(string)',
	'IsExcluded(boolean)'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	offers._ID,
	'false'
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
WHERE
	opp_d.MATCH_STATUS = 'Full Match'
	AND offers.ISEXCLUDED = 'true'
	AND wda.SS_STATUS = 'Not Processed'
	AND offers._ID IS NOT NULL

INTO OUTFILE '${EX_DIR}/polycom_disassociate_resolve_as_loss_full_match_update_excluded_offers_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
SELECT NOW(), ' Executing Step 4.4.1' from DUAL;
/*
**	4.4.1 Produce the \"resolveAsLoss\" Scrub (WEEKLY_DISASSOCIATED_ASSETS)
**	Appendix 4
*/
SELECT
	'Name', /* '_id(string)', */
	'Reason', /* 'resultReason(string)' */
	'LossDate' /* 'resolutionDate(date)', */
FROM
	DUAL
UNION ALL
SELECT
	COALESCE(opp_d.OPP_ID, ''),
	'haPRT', -- 'HA – Product Return - PRT'
	COALESCE(MIN(wda.ASSET_DISASSOCIATION_DT), '')
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
WHERE
	opp_d.MATCH_STATUS = 'Full Match'
	AND wda.SS_STATUS = 'Not Processed'
	AND opp_d.OPP_ID IS NOT NULL
GROUP BY
	opp_d.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_disassociate_resolve_as_loss_full_match_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 4.5.0' from DUAL;
/*
**	4.5 Produce the the scrub 'IsExcluded' offers (WEEKLY_DISASSOCIATED_ASSETS)
**	Appendix 4
*/
SELECT
	'_id(string)',
	'IsExcluded(boolean)'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	offers._ID,
	'false'
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
WHERE
	opp_d.MATCH_STATUS = 'Partial Match'
	AND offers.ISEXCLUDED = 'true'
	AND wda.SS_STATUS = 'Not Processed'
	AND offers._ID IS NOT NULL

INTO OUTFILE '${EX_DIR}/polycom_disassociate_resolve_as_loss_partial_match_update_excluded_offers_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
SELECT NOW(), ' Executing Step 4.5.1' from DUAL;
/*
**	4.5.1 Produce the scrub for partial matched portion of split opportunity (WEEKLY_DISASSOCIATED_ASSETS)
**	Appendix 4
*/
SELECT
	'Name', /* '_id(string)', */
	'Reason', /* 'resultReason(string)' */
	'LossDate' /* 'resolutionDate(date)', */
FROM
	DUAL
UNION ALL
SELECT
	opp_d.OPP_ID,
	'haPRT', -- 'HA – Product Return - PRT'
	COALESCE(MIN(wda.ASSET_DISASSOCIATION_DT), '')
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
WHERE
	opp_d.MATCH_STATUS = 'Partial Match'
	AND wda.SS_STATUS = 'Not Processed'
	AND opp_d.OPP_ID IS NOT NULL
GROUP BY
	opp_d.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_disassociate_resolve_as_loss_partial_match_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 4.6' from DUAL;
/*
**	4.6 Produce the \"SPLIT Opportunity\" unmatched portion (WEEKLY_DISASSOCIATED_ASSETS)
**	Appendix 3
*/
SELECT
	'Opportunity',
	'OfferIds'
FROM
	DUAL
UNION ALL
SELECT
	opp_d.OPP_ID,
	group_concat(offers._ID)
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_BASE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
	AND wda.SS_STATUS = 'Not Processed'
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = opp_d.OPP_ID
	AND opp.ISSUBORDINATE <> 'true'
WHERE
	opp_d.MATCH_STATUS = 'Partial Match'
	AND wda.TMP_ID is NULL
	AND opp_d.OPP_ID IS NOT NULL
	AND offers._ID IS NOT NULL
GROUP BY
	opp_d.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_disassociate_split_opportunity_unmatched_offers_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 4.7' from DUAL;
/*
**	4.7 Recalculate Opportunity for partial matched portion of split opportunity (WEEKLY_DISASSOCIATED_ASSETS)
**	Appendix 8
*/
SELECT
	'_id(string)',
	'amount.amount(numeric)',
	'targetAmount.amount(numeric)',
	'extensions.tenant.priorRenewalAmountNotAnnualized.value.amount(numeric)',
	'extentions.tenant.listPrice.value.amount(numeric)',
	'targetDate(date)',
	'earliestNewStartDate(date)',
	'latestNewEndDate(date)'
FROM
	DUAL
UNION ALL
SELECT
	opp_d.OPP_ID,
	COALESCE(SUM(offers.AMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.TARGETAMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_PRIORRENEWALAMOUNTNOTANNUALIZED_VALUE_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_LISTPRICE_AMOUNT), ''),
	COALESCE(MIN(offers.TARGETDATE), ''),
	COALESCE(MIN(offers.STARTDATE), ''),
	COALESCE(MAX(offers.ENDDATE), '')
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
WHERE
	opp_d.MATCH_STATUS in ('Matched', 'Partial Match')
	AND wda.SS_STATUS = 'Not Processed'
	AND opp_d.OPP_ID IS NOT NULL
GROUP BY
	opp_d.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_disassociate_resolve_as_loss_recalculate_opportunity_match_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 4.8' from DUAL;
/*
**	4.8 Produce the \"SPLIT Opportunity\" unmatched portion (WEEKLY_DISASSOCIATED_ASSETS)
**	Appendix 8
*/
SELECT
	'_id(string)',
	'amount.amount(numeric)',
	'targetAmount.amount(numeric)',
	'extensions.tenant.priorRenewalAmountNotAnnualized.value.amount(numeric)',
	'extentions.tenant.listPrice.value.amount(numeric)',
	'targetDate(date)',
	'earliestNewStartDate(date)',
	'latestNewEndDate(date)'
FROM
	DUAL
UNION ALL
SELECT
	opp_d.OPP_ID,
	COALESCE(SUM(offers.AMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.TARGETAMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_PRIORRENEWALAMOUNTNOTANNUALIZED_VALUE_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_LISTPRICE_AMOUNT), ''),
	COALESCE(MIN(offers.TARGETDATE), ''),
	COALESCE(MIN(offers.STARTDATE), ''),
	COALESCE(MAX(offers.ENDDATE), '')
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
	AND wda.SS_STATUS = 'Not Processed'
WHERE
	opp_d.MATCH_STATUS = 'Partial Match'
	AND wda.TMP_ID is NULL
	AND opp_d.OPP_ID IS NOT NULL
GROUP BY
	opp_d.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_disassociate_resolve_as_loss_recalculate_opportunity_partial_unmatched_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	4.8.1 \"Resolve Back to Original Sales Stage\" reopened \"split\" opportunity
**	Appendex 11
*/
SELECT
	'Name', /* '_id(string)', */
	'Reason', /* 'resultReason(string)' */
	'LossDate' /* 'resolutionDate(date)', */
FROM
	DUAL
UNION ALL
SELECT
	opp_d.OPP_ID,
	COALESCE(MIN(offers.RESULTREASON_NAME), ''),
	COALESCE(opp.RESOLUTIONDATE, '')
FROM
	polycom_tmp.opp_disassociate_tmp opp_d
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_d.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
LEFT OUTER JOIN polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
	ON offers.EXTENSIONS_TENANT_ASSETID_VALUE = wda.ASSET
	AND offers.EXTENSIONS_TENANT_SERIALNUMBER_VALUE = wda.SERIAL_NUM
	AND DATE(offers.TARGETDATE) = wda.ENTITLEMENT_END_DT
	AND offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE = wda.AGREE_LINE_SERVICE_PART_NUM
	AND offers.EXTENSIONS_TENANT_ENTITLEID_VALUE = wda.ENTITLEMENT
WHERE
	opp_d.MATCH_STATUS = 'Partial Match'
	AND opp_d.IS_REOPENED = true
	AND wda.SS_STATUS = 'Not Processed'
	AND wda.TMP_ID is NULL
	AND opp_d.OPP_ID IS NOT NULL
GROUP BY
	opp_d.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_disassociate_resolve_as_loss_reset_back_reopened_opportunities_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 4.9' from DUAL;
/*
**	4.9 Produce the exception report
*/
SELECT
	'EXCEPTION',
	'INTEGRATION_ID',
	'ASSET',
	'ASSET_NUM',
	'SERIAL_NUM',
	'ENTITLEMENT',
	'AGREEMENT',
	'PRODUCT',
	'AGREE_LINE_SERVICE_PART_NUM',
	'OPERATION_CD',
	'ASSET_DISASSOCIATION_DT',
	'PROD_INT_ID',
	'AGREEMENT_ITEM',
	'AGREEMENT_VALID_FLG',
	'AGREEMENT_ACCOUNT',
	'ASSET_OWNER_ACCOUNT',
	'ENTITLEMENT_START_DT',
	'ENTITLEMENT_END_DT',
	'AGREE_START_DT',
	'AGREE_END_DT'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	case
		/* when wdat.EXCEPTION_ID = 0 then 'Valid Data' */
		when wdat.EXCEPTION_ID = 1 then 'Failed to match'
		when wdat.EXCEPTION_ID = 2 then 'Matched - but already Closed Sale or House Account'
		when wdat.EXCEPTION_ID = 3 then 'Matched - but no Resolution Date'
		when wdat.EXCEPTION_ID = 4 then 'Multiple Offers Matched'
		else ''
	end as EXCEPTION_MSG,
	COALESCE(INTEGRATION_ID, ''),
	COALESCE(ASSET, ''),
	COALESCE(ASSET_NUM, ''),
	COALESCE(SERIAL_NUM, ''),
	COALESCE(ENTITLEMENT, ''),
	COALESCE(AGREEMENT, ''),
	COALESCE(PRODUCT, ''),
	COALESCE(AGREE_LINE_SERVICE_PART_NUM, ''),
	COALESCE(OPERATION_CD, ''),
	COALESCE(ASSET_DISASSOCIATION_DT, ''),
	COALESCE(PROD_INT_ID, ''),
	COALESCE(AGREEMENT_ITEM, ''),
	COALESCE(AGREEMENT_VALID_FLG, ''),
	COALESCE(AGREEMENT_ACCOUNT, ''),
	COALESCE(ASSET_OWNER_ACCOUNT, ''),
	COALESCE(ENTITLEMENT_START_DT, ''),
	COALESCE(ENTITLEMENT_END_DT, ''),
	COALESCE(AGREE_START_DT, ''),
	COALESCE(AGREE_END_DT, '')
FROM
	polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
INNER JOIN polycom_tmp.weekly_disassociated_assets_tmp wdat
	ON wda.TMP_ID = wdat.TMP_ID
WHERE
	wdat.EXCEPTION_ID > 0
	${DISASSOCIATE_EXCEPTION_DATE_CLAUSE}

INTO OUTFILE '${EX_DIR}/polycom_disassociated_assets_exception_report_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
SELECT NOW(), ' Executing Step 4.10' from DUAL;
/*
**	4.10 Update the processed status of all 'Not Processed' WEEKLY_DISASSOCIATED_ASSETS
**	This will prevent us from reprocessing these records again.
*/
SET autocommit=0;
UPDATE polycom_data.WEEKLY_DISASSOCIATED_ASSETS wda
INNER JOIN polycom_tmp.weekly_disassociated_assets_tmp wdat
	ON wda.TMP_ID = wdat.TMP_ID
SET
	wda.SS_STATUS = 'Processed',
	wda.SS_PROCESSED_DT = NOW()
WHERE
	wdat.EXCEPTION_ID IN (0, 2)
	AND wda.SS_STATUS = 'Not Processed';
commit;
/*
**=====</step 4>=====
*/

SELECT NOW(), ' Polycom resolve as loss script completed' from DUAL;
"
