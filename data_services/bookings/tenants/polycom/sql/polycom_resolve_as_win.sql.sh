#!/bin/bash

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${BASE_DIR}/polycom_resolve_common.sql.sh

#
# 'Unmatched' exception report only for the processing date
#
ORDER_EXCEPTION_DATE_CLAUSE=""
ENTITLEMENT_EXCEPTION_DATE_CLAUSE=""

if [[ "$REPORT_ALL_EXCEPTIONS" == "1" ]]
then
	echo "$(date) Exceptions for all 'Not Processed' records will be reported"
else
	echo "$(date) Exceptions for 'Not Processed' records of '$PROCESS_DATE' will be reported"
	ORDER_EXCEPTION_DATE_CLAUSE="AND doa.SS_IMPORT_DT='$PROCESS_DATE'"
	ENTITLEMENT_EXCEPTION_DATE_CLAUSE="AND e.SS_IMPORT_DT='$PROCESS_DATE'"
fi

#
# -s -s (two of them) are purposefully used
#
mysql $SCHEMA -s -s -e "
/*
** 1. Create temporary tables so that it is easier to process
** 2. For each use case create a separate script
*/
SET tmp_table_size=64000000;
SET max_heap_table_size=64000000;
SET group_concat_max_len=64000;

SELECT NOW(), ' Polycom resolve as win script started' from DUAL;
CREATE DATABASE IF NOT EXISTS polycom_tmp default character set utf8;

/*
**=====<step 1>=====
**	1.0 Matching for ENTITLEMENT_DAILY_ACTIVITY
*/
DROP TABLE IF EXISTS polycom_tmp.entitlement_daily_activity_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.entitlement_daily_activity_tmp (
	TMP_ID			INTEGER NOT NULL,
	MATCH_STATUS		varchar(50),
	OFFER_ID		varchar(50),
	EXCEPTION_ID		INTEGER DEFAULT 0,
	OFFER_AMOUNT		DECIMAL(10,4),
	KEY ix_tmp_id (TMP_ID),
	KEY ix_match (MATCH_STATUS)
) ENGINE=InnoDB DEFAULT character set utf8;

DROP TABLE IF EXISTS polycom_tmp.reactivation_fee;
CREATE TABLE IF NOT EXISTS polycom_tmp.reactivation_fee (
	OPP_ID			varchar(50) NOT NULL,
	DE_ID			INTEGER NOT NULL,
	AGREE_PO		varchar(50),
	REACTIVATION_FEE	DECIMAL(10,4),
	KEY ix_opp_id (OPP_ID)
) ENGINE=InnoDB DEFAULT character set utf8;

SELECT NOW(), ' Executing Step 1.1' from DUAL;
/*
**	1.1 NOT MATCHED (ENTITLEMENT_DAILY_ACTIVITY)
**	Appendex 1
*/
SET autocommit=0;
INSERT INTO polycom_tmp.entitlement_daily_activity_tmp (
	TMP_ID,
	MATCH_STATUS,
	OFFER_ID
)
SELECT DISTINCT
	pai.T_ID,
	'Not Matched',
	NULL
FROM
	( SELECT DISTINCT
		eda.TMP_ID as T_ID
	FROM
		polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	LEFT OUTER JOIN ${SCHEMA}.APP_OFFERS offers
		ON eda.PREV_AGREE_ID = offers.EXTENSIONS_TENANT_AGREEID_VALUE
		AND eda.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
		AND eda.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
		AND eda.PREV_ENTITLEMENT_ID = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
	LEFT OUTER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers /* only look at active offers */
		ON offers._ID = t_offers.OFFID
	WHERE
		t_offers.OFFID IS NULL
		AND eda.SS_STATUS = 'Not Processed') pai
INNER JOIN
	( SELECT DISTINCT
		eda.TMP_ID as T_ID
	FROM
		polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	LEFT OUTER JOIN ${SCHEMA}.APP_OFFERS offers
		ON eda.PREV_ENTL_PO = offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE
		AND eda.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
		AND eda.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
		AND eda.PREV_ENTITLEMENT_ID = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
	LEFT OUTER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers /* only look at active offers */
		ON offers._ID = t_offers.OFFID
	WHERE
		t_offers.OFFID IS NULL
		AND eda.SS_STATUS = 'Not Processed') pep
	ON pai.T_ID = pep.T_ID;
commit;

SELECT NOW(), ' Executing Step 1.1.1' from DUAL;
/*
**	1.1.1 Update 'Polycom Booking Date' for the 'Re-activation' opportunities
**	This generates a scrub file
**	Appendix 12
**	Since these are re-activated and created as a result of mysql extracts the AGREE_PO will match with
**	offers' AGREE_PO (2014-07-20)
*/
SELECT
	'_id(string)',
	'extensions.tenant.polycomBookingDate(date)'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	opp._ID,
	COALESCE(MIN(doa.SO_DATE), '')
FROM
	${SCHEMA}.T_ACTIVE_OFFERS t_offers
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
INNER JOIN polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY doa
	ON doa.AGREE_PO = offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE /* don't change this to ENTITLEMENT AGREE_PO */
WHERE
	offers.EXTENSIONS_TENANT_ASSETID_VALUE like '%Re-activation%'
	AND opp.FLOWS_SALESSTAGES_STATE_NAME NOT IN ( 'closedSale', 'noService', 'houseAccount' )
	AND opp._ID IS NOT NULL
GROUP BY
	opp._ID

INTO OUTFILE '${EX_DIR}/polycom_entitlement_resolve_as_win_reactivated_opportunities_update_booking_date_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
/*
**	1.1.2 Close out the opportunities opened just for \"Re-activation\" fee previous day
**	This generates a scrub file
**	Appendix 12
**	Since these are re-activated and created as a result of mysql extracts the AGREE_PO will match with
**	offers' AGREE_PO (2014-07-20)
*/
SELECT NOW(), ' Executing Step 1.1.2' from DUAL;
SELECT
	'Name',
	'resolutionDate',
	'poAmount',
	'poDate',
	'poNumber',
	'reason',
	'soAmount',
	'soDate',
	'soNumber'
FROM
	DUAL
UNION ALL
SELECT
	opp._ID,
	COALESCE(MIN(doa.SO_DATE), ''),
	COALESCE(SUM(offers.AMOUNT_AMOUNT), ''),
	COALESCE(MIN(doa.SO_DATE), ''),
	COALESCE(MIN(doa.AGREE_PO), ''),
	'csRCT', /* \"CS - Re-cert Fee Included - RCT\" */
	COALESCE(SUM(offers.AMOUNT_AMOUNT), ''),
	COALESCE(MIN(doa.SO_DATE), ''),
	IF(LENGTH(MIN(doa.AGREE_SO)) = 0, 'Blank', MIN(doa.AGREE_SO))
FROM
	${SCHEMA}.T_ACTIVE_OFFERS t_offers
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
INNER JOIN polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY doa
	ON doa.AGREE_PO = offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE /* don't change this to ENTITLEMENT AGREE_PO */
WHERE
	offers.EXTENSIONS_TENANT_ASSETID_VALUE like '%Re-activation%'
	AND opp.FLOWS_SALESSTAGES_STATE_NAME NOT IN ( 'closedSale', 'noService', 'houseAccount' )
	AND opp._ID IS NOT NULL
GROUP BY
	opp._ID

INTO OUTFILE '${EX_DIR}/polycom_entitlement_resolve_as_win_reactivated_opportunities_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 1.2' from DUAL;
/*
**	1.2 MATCHED (ENTITLEMENT_DAILY_ACTIVITY)
**	Appendex 1
*/
SET autocommit=0;
INSERT INTO polycom_tmp.entitlement_daily_activity_tmp (
	TMP_ID,
	MATCH_STATUS,
	OFFER_ID
)
SELECT DISTINCT
	eda.TMP_ID,
	'Matched',
	t_offers.OFFID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON eda.PREV_AGREE_ID = offers.EXTENSIONS_TENANT_AGREEID_VALUE
	AND eda.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
	AND eda.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND eda.PREV_ENTITLEMENT_ID = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON offers._ID = t_offers.OFFID
WHERE
	eda.SS_STATUS = 'Not Processed'
UNION
SELECT DISTINCT
	eda.TMP_ID,
	'Matched',
	t_offers.OFFID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON eda.PREV_ENTL_PO = offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE
	AND eda.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
	AND eda.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND eda.PREV_ENTITLEMENT_ID = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON offers._ID = t_offers.OFFID
WHERE
	eda.SS_STATUS = 'Not Processed';
commit;

/*
**	1.2.1 MATCHED (ENTITLEMENT_DAILY_ACTIVITY)
**	Appendex 1
**	Multiple Offers matched exception
*/
DROP TABLE IF EXISTS polycom_tmp.ent_id_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.ent_id_tmp (
	TMP_ID	INTEGER
) ENGINE=MEMORY DEFAULT character set utf8;

SET autocommit=0;
INSERT INTO polycom_tmp.ent_id_tmp
SELECT
	edat.TMP_ID
FROM
	polycom_tmp.entitlement_daily_activity_tmp edat
GROUP BY
	edat.TMP_ID, edat.MATCH_STATUS
HAVING
	COUNT(1) > 1;

UPDATE
	polycom_tmp.entitlement_daily_activity_tmp edat
INNER JOIN polycom_tmp.ent_id_tmp eit
	ON eit.TMP_ID = edat.TMP_ID
SET
	edat.MATCH_STATUS = 'Multiple Offers Matched';
commit;
/*
**	Done with this table. Drop it.
*/
DROP TABLE IF EXISTS polycom_tmp.ent_id_tmp;

SELECT NOW(), ' Executing Step 1.3' from DUAL;
/*
**	1.3 Calculate the Offer Amount from ENTITLEMENT table.
**	Appendix 7
*/
UPDATE polycom_tmp.entitlement_daily_activity_tmp edat
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.TMP_ID = edat.TMP_ID
	AND edat.MATCH_STATUS = 'Matched'
SET
	edat.OFFER_AMOUNT = COALESCE(eda.AGREE_LINE_PART_NET_PER_ASSET, 0)
;
commit;

SELECT NOW(), ' Executing Step 1.4' from DUAL;
/*
**	1.4 Produce 'Update Offer with values in ENTITLEMENT'
**	Appendix 7
*/
SELECT
	'_id(string)',
	'amount.amount(numeric)',
	'amount.code(string)',
	'startDate(date)',
	'endDate(date)',
	'extensions.tenant.bundledServicePartNumber.value(string)',
	'extensions.tenant.unbundledServicePartNumber.value(string)',
	'isExcluded(boolean)'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	edat.OFFER_ID,
	COALESCE(eda.AGREE_LINE_PART_NET_PER_ASSET, ''),
	LOWER(COALESCE(eda.AGREE_LINE_CURRENCY, eda.SO_CURRENCY, '')),
	COALESCE(eda.ENTL_ST_DATE, ''),
	COALESCE(eda.ENTL_END_DATE, ''),
	COALESCE(eda.AGREE_LINE_SERVICE_PART_NUM, ''),
	COALESCE(eda.BUNDLE_PART_TRNS, ''),
	'false'
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.TMP_ID = eda.TMP_ID
	AND edat.MATCH_STATUS = 'Matched'
WHERE
	edat.OFFER_ID IS NOT NULL

INTO OUTFILE '${EX_DIR}/polycom_entitlement_update_offers_with_values_in_entitlement_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 1.5' from DUAL;
/*
**	1.5 Produce Service Product relationship if different from the one in ENTITLEMENT
**	Appendix 7
*/
SELECT
	'Source',
	'Target'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	offers._ID,
	products._ID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.TMP_ID = eda.TMP_ID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = edat.OFFER_ID
	AND offers.PRODUCT_DISPLAYNAME <> eda.BUNDLE_PART_TRNS
INNER JOIN ${SCHEMA}.APP_PRODUCTS products
	ON products.DISPLAYNAME = offers.PRODUCT_DISPLAYNAME
WHERE
	offers._ID IS NOT NULL
	AND products._ID IS NOT NULL

INTO OUTFILE '${EX_DIR}/polycom_entitlement_update_offers_product_relationship_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**=====</Step 1>=====
*/
/*
**=====<step 2>=====
**	2.0 Process Matched Offers (ENTITLEMENT_DAILY_ACTIVITY)
*/

DROP TABLE IF EXISTS polycom_tmp.opp_entitlement_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.opp_entitlement_tmp (
	OPP_ID			VARCHAR(50),
	MATCH_STATUS		VARCHAR(50),
	IS_REOPENED		BOOLEAN DEFAULT false,
	RESOLUTION_DATE		DATE,
	RESOLUTION_REASON	VARCHAR(50),
	KEY ix_opp_id_a_match_status (OPP_ID, MATCH_STATUS),
	KEY ix_match_status (MATCH_STATUS)
) ENGINE=InnoDB DEFAULT character set utf8;

SELECT NOW(), ' Executing Step 2.1' from DUAL;
/*
**	2.1.1 Reopen 'noService' and 'houseAccount' opportunities (ENTITLEMENT_DAILY_ACTIVITY)
**	Appendix 9
*/
SELECT '_id(string)'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	opp._ID
FROM
	polycom_tmp.entitlement_daily_activity_tmp edat
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON edat.MATCH_STATUS = 'Matched'
	AND t_offers.OFFID = edat.OFFER_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME IN ('houseAccount', 'noService')
	AND opp._ID IS NOT NULL

INTO OUTFILE '${EX_DIR}/polycom_entitlement_resolve_as_win_reopen_opportunity_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
/*
**	2.1.2 Find out all matching opportunities (ENTITLEMENT_DAILY_ACTIVITY)
*/
SET autocommit=0;
INSERT INTO polycom_tmp.opp_entitlement_tmp (
	OPP_ID,
	MATCH_STATUS
)
SELECT DISTINCT
	opp._ID,
	NULL
FROM
	polycom_tmp.entitlement_daily_activity_tmp edat
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON edat.MATCH_STATUS = 'Matched'
	AND t_offers.OFFID = edat.OFFER_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME <> 'closedSale'
	AND opp._ID IS NOT NULL;
/*
**	2.1.3 Mark the reopened opportunities (ENTITLEMENT_DAILY_ACTIVITY)
*/
UPDATE	polycom_tmp.opp_entitlement_tmp oet
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = oet.OPP_ID
SET
	oet.IS_REOPENED = true
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME IN ('houseAccount', 'noService');
commit;


SELECT NOW(), ' Executing Step 2.2' from DUAL;
/*
**	2.2 Find out the partially matching opportunities (ENTITLEMENT_DAILY_ACTIVITY)
*/
DROP TABLE IF EXISTS polycom_tmp.opp_id_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.opp_id_tmp (
	OPP_ID	varchar(50),
	KEY ix_opp (OPP_ID)
) ENGINE=InnoDB DEFAULT character set utf8;

SET autocommit=0;
INSERT INTO polycom_tmp.opp_id_tmp
SELECT DISTINCT
	opp_e.OPP_ID
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.PREV_AGREE_ID = offers.EXTENSIONS_TENANT_AGREEID_VALUE
	AND eda.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
	AND eda.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND eda.PREV_ENTITLEMENT_ID = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
WHERE
	eda.TMP_ID IS NULL
UNION
SELECT DISTINCT
	opp_e.OPP_ID
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.PREV_ENTL_PO = offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE
	AND eda.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
	AND eda.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND eda.PREV_ENTITLEMENT_ID = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
WHERE
	eda.TMP_ID IS NULL
;
commit;

SET autocommit=0;
UPDATE polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN polycom_tmp.opp_id_tmp oit
	ON opp_e.OPP_ID = oit.OPP_ID
SET
	opp_e.MATCH_STATUS = 'Partial Match';
commit;
TRUNCATE polycom_tmp.opp_id_tmp;

SELECT NOW(), ' Executing Step 2.2' from DUAL;
/*
**	2.3 Find out the Fully matching opportunities (ENTITLEMENT_DAILY_ACTIVITY)
*/
SET autocommit=0;
INSERT INTO polycom_tmp.opp_id_tmp
SELECT DISTINCT
	opp_e.OPP_ID
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.PREV_AGREE_ID = offers.EXTENSIONS_TENANT_AGREEID_VALUE
	AND eda.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
	AND eda.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND eda.PREV_ENTITLEMENT_ID = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
WHERE
	opp_e.MATCH_STATUS IS NULL
UNION
SELECT DISTINCT
	opp_e.OPP_ID
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.PREV_ENTL_PO = offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE
	AND eda.AGREE_LINE_SERVICE_PART_NUM = offers.EXTENSIONS_TENANT_BUNDLEDSERVICEPARTNUMBER_VALUE
	AND eda.ASSET_ID = offers.EXTENSIONS_TENANT_ASSETID_VALUE
	AND eda.PREV_ENTITLEMENT_ID = offers.EXTENSIONS_TENANT_ENTITLEID_VALUE
WHERE
	opp_e.MATCH_STATUS IS NULL
;
commit;

SET autocommit=0;
UPDATE polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN polycom_tmp.opp_id_tmp oit
	ON opp_e.OPP_ID = oit.OPP_ID
SET
	opp_e.MATCH_STATUS = 'Full Match';
commit;
TRUNCATE polycom_tmp.opp_id_tmp;
/*
**====</Step 2>=====
*/

SELECT NOW(), ' Executing Step 3.0' from DUAL;
/*
**=====<Step 3>=====
**	3.0 Produce the Update \"IsExcluded\" scrub (ENTITLEMENT_DAILY_ACTIVITY)
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
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
WHERE
	opp_e.MATCH_STATUS = 'Full Match'
	AND offers.ISEXCLUDED = 'true'
	AND offers._ID IS NOT NULL

INTO OUTFILE '${EX_DIR}/polycom_entitlement_resolve_as_win_full_match_update_excluded_offers_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 3.1.0' from DUAL;
/*
**	3.1.0 Calculate 'Result Reason' (ENTITLEMENT_DAILY_ACTIVITY)
*/
DROP TABLE IF EXISTS polycom_tmp.close_sale_result_reason;
CREATE TABLE IF NOT EXISTS polycom_tmp.close_sale_result_reason (
	OPP_ID	varchar(50),
	OFFER_ID	varchar(50),
	RESULT_REASON	varchar(50),
	INDEX ix_result(OPP_ID, OFFER_ID, RESULT_REASON)
) ENGINE=InnoDB DEFAULT character set utf8;

DROP TABLE IF EXISTS polycom_tmp.close_sale_use_reason;
CREATE TABLE IF NOT EXISTS polycom_tmp.close_sale_use_reason (
	OPP_ID	varchar(50),
	RESULT_REASON	varchar(50),
	INDEX ix_use(OPP_ID)
) ENGINE=InnoDB DEFAULT character set utf8;

SET autocommit=0;
INSERT INTO polycom_tmp.close_sale_result_reason (
	OPP_ID,
	OFFER_ID,
	RESULT_REASON
)
SELECT DISTINCT
	opp_e.OPP_ID,
	offers._ID,
	case
		when DATEDIFF(offers.ENDDATE, offers.STARTDATE) < 365
			then 'csCTS' -- \"CS - Co-term Short - CTS\"
		when DATEDIFF(offers.ENDDATE, offers.STARTDATE) > 365
			then 'csCTL' -- \"CS - Co-term Long - CTL\"
		when DATEDIFF(offers.ENDDATE, offers.STARTDATE) = 365
			then 'csRAP' -- \"CS - Renewed at Par - R@P\"
	end as ResultReason
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID;
/*
**	Identify opportunities/offers with Re-activation fee
**	'csRCT' -- \"CS - Re-Cert Fee Included - RCT\"
**	Appendix 6
*/
INSERT INTO polycom_tmp.close_sale_result_reason (
	OPP_ID,
	OFFER_ID,
	RESULT_REASON
)
SELECT DISTINCT
	opp_e.OPP_ID,
	offers._ID,
	'csRCT' -- \"CS - Re-Cert Fee Included - RCT\"
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN Polycom_UAT.APP_OPPORTUNITIES opp
	ON t_offers.OPPID = opp._ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = offers._ID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.TMP_ID = edat.TMP_ID
INNER JOIN polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY doa
	ON doa.AGREE_PO = eda.AGREE_PO /* offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE */
LEFT OUTER JOIN polycom_data.REACTIVATED_AGREE_PO rap
	ON doa.AGREE_PO = rap.AGREE_PO
WHERE
	doa.AGREE_LINE_PRODUCT_GROUP = '2015'
	AND doa.AGREE_PO IS NOT NULL
	AND doa.AGREE_PO NOT LIKE '%BUNDLE%'
	AND doa.AGREE_LINE_SERVICE_PRODUCT LIKE '%fee%'
	AND opp.FLOWS_SALESSTAGES_STATE_NAME <> 'closedSale'
	AND opp_e.OPP_ID IS NOT NULL
	AND rap.TMP_ID IS NULL;
/*
**	Calculate reactivation fee for opportunities
**	Appendix 6
*/
INSERT INTO polycom_tmp.reactivation_fee (
	OPP_ID,
	DE_ID,
	AGREE_PO,
	REACTIVATION_FEE
)
SELECT DISTINCT
	opp_e.OPP_ID,
	MIN(eda.TMP_ID),
	MIN(doa.AGREE_PO),
	SUM(doa.SO_EXT_NET_PRICE_USD)
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN Polycom_UAT.APP_OPPORTUNITIES opp
	ON t_offers.OPPID = opp._ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = offers._ID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.TMP_ID = edat.TMP_ID
INNER JOIN polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY doa
	ON doa.AGREE_PO = eda.AGREE_PO /* offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE */
LEFT OUTER JOIN polycom_data.REACTIVATED_AGREE_PO rap
	ON doa.AGREE_PO = rap.AGREE_PO
WHERE
	doa.AGREE_LINE_PRODUCT_GROUP = '2015'
	AND doa.AGREE_PO IS NOT NULL
	AND doa.AGREE_PO NOT LIKE '%BUNDLE%'
	AND doa.AGREE_LINE_SERVICE_PRODUCT LIKE '%fee%'
	AND opp.FLOWS_SALESSTAGES_STATE_NAME <> 'closedSale'
	AND opp_e.OPP_ID IS NOT NULL
	AND rap.TMP_ID IS NULL
GROUP BY
	opp_e.OPP_ID;
commit;
/*
**	Maintain previously reactivated AGREE_PO ids
**	We need it in two tables, one for processing and the other for historical purposes.
**	Appendix 6
*/
DROP TABLE IF EXISTS polycom_tmp.agree_po_tmp;
CREATE TABLE IF NOT EXISTS polycom_tmp.agree_po_tmp (
	AGREE_PO		varchar(50) NOT NULL PRIMARY KEY,
	SS_IMPORT_DT	DATE
) ENGINE=MEMORY DEFAULT character set utf8;

SET autocommit=0;
INSERT INTO polycom_tmp.agree_po_tmp (
	AGREE_PO,
	SS_IMPORT_DT
)
SELECT DISTINCT
	doa.AGREE_PO,
	doa.SS_IMPORT_DT
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN Polycom_UAT.APP_OPPORTUNITIES opp
	ON t_offers.OPPID = opp._ID
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = offers._ID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.TMP_ID = edat.TMP_ID
INNER JOIN polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY doa
	ON doa.AGREE_PO = eda.AGREE_PO /* offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE */
LEFT OUTER JOIN polycom_data.REACTIVATED_AGREE_PO rap
	ON doa.AGREE_PO = rap.AGREE_PO
WHERE
	doa.AGREE_LINE_PRODUCT_GROUP = '2015'
	AND doa.AGREE_PO IS NOT NULL
	AND doa.AGREE_PO NOT LIKE '%BUNDLE%'
	AND doa.AGREE_LINE_SERVICE_PRODUCT LIKE '%fee%'
	AND opp.FLOWS_SALESSTAGES_STATE_NAME <> 'closedSale'
	AND rap.TMP_ID IS NULL;

INSERT INTO polycom_data.REACTIVATED_AGREE_PO (
	AGREE_PO,
	SS_IMPORT_DT
)
SELECT
	AGREE_PO,
	SS_IMPORT_DT
FROM
	polycom_tmp.agree_po_tmp;
commit;

/*
**	Calculate reactivation fee and export the corresponding ENTITLEMENT record
**	so that data load and processing in renew produce an opportunity that can be
**	closed during next processing iteration.
**	Appendix 10
*/
SELECT
'ANNUALIZED_VALUE','BATCH_QUARTER','AGREE_SALES_REGION','THEATER','COUNTRY','CUSTOMER','AGREE_LINE_CURRENCY','AGREE_NUM','ASSET_SERIAL_NUM','AGREE_MASTER_ACCNT_ID','ENTL_END_DATE','ASSET_OWNER_ADDRESS_ID','AGREE_PO','BUNDLE_PART_TRNS','MASTER_ACCNT_RES_ID','ENTL_ST_DATE','ASSET_PRODUCT_DIVISION','ASSET_PRODUCT_LINE','ASSET_PRODUCT_GROUP','MSRP','EOSL','AGREE_ID','NO_OF_ASSET_ENTL_ON_LINE','AGREE_LINE_ID','ASSET_ID','ASSET_SHIP_DATE','ASSET_SHIP_YEAR','ASSET_PO_NUM','ASSET_SO_NUM','ASSET_PART_NUM','ASSET_PRODUCT_ID','ENTL_CREATE_DATE','ENTL_ID','ENTL_NAME','AGREE_MSTR_ACCNT_NAME','AGREE_MSTR_END_CUST_ID','END_USER_MSTR_ACCT_NAME','MSTR_ACCT_RES_NAME','ASSET_OWNER_ID','AGREE_LINE_QTY','ENTL_NET_PRICE_PER_ASSET','DISCOUNT_CODE','AGREE_SO','SO_CURRENCY','OWNERSHIP','MKTG_NAME','EOSL_DATE','AGREE_LINE_SERVICE_PART_NUM','EXTERNAL_ENTL_END_DATE','EXTERNAL_ENTL_CREATE_DATE','ASSET_OWNER_ADDRESS_1','ASSET_OWNER_COUNTRY','ASSET_OWNER_POSTAL','TERRITORY','PORTAL_PRIMARY_AGREE_NAME','PIM_DESCRIPTION'
FROM
        DUAL
UNION ALL
SELECT
        ifnull(rf.REACTIVATION_FEE,'') as REACTIVATION_FEE,
        ifnull(e.ENTL_BATCH_QUARTER,'') as ENTL_BATCH_QUARTER,

        case when e.AGREE_SALES_REGION='internal' then 'ukEire' when e.AGREE_SALES_REGION IN('Germany','Turkey','Italy')
        then LOWER(e.AGREE_SALES_REGION) else ifnull(R.name,e.AGREE_SALES_REGION) end as AGREE_SALES_REGION,

        ifnull(T.name,e.THEATER) As THEATER,

        case when e.ASSET_OWNER_ID not In ('0','','Unspecified') and e.ASSET_OWNER_COUNTRY not IN ('0','','Unspecified') then  e.ASSET_OWNER_COUNTRY
        when e.ASSET_OWNER_ID not In ('0','','Unspecified') and e.ASSET_OWNER_COUNTRY IN ('0','','Unspecified') then  e.AGREE_ACCNT_COUNTRY

        when e.ASSET_OWNER_ID In ('0','','Unspecified') and e.AGREE_END_CUST_ID  NOT In ('0','','Unspecified') and e.AGREE_END_CUST_COUNTRY  not IN ('0','','Unspecified') then e.AGREE_END_CUST_COUNTRY

        when e.ASSET_OWNER_ID In ('0','','Unspecified') and e.AGREE_END_CUST_ID  NOT In ('0','','Unspecified') and e.AGREE_END_CUST_COUNTRY IN ('0','','Unspecified')
        then e.AGREE_ACCNT_COUNTRY
        else ifnull(e.AGREE_ACCNT_COUNTRY,'') end as COUNTRY,

        case when e.ASSET_OWNER_ID not In ('0','','Unspecified') then e.ASSET_OWNER_ID
        when e.ASSET_OWNER_ID In ('0','','Unspecified') and e.AGREE_END_CUST_ID  NOT In ('0','','Unspecified') then e.AGREE_END_CUST_ID
        else ifnull(replace(e.AGREE_ACCNT_ID,'Unspecified',''),'') end As CUSTOMER,
        'usd',
        ifnull(e.AGREE_NUM,'') as AGREE_NUM,
        'Re-activation Fee',
        ifnull(e.AGREE_MASTER_ACCNT_ID,'') as AGREE_MASTER_ACCNT_ID,
        Case When e.ENTL_END_DATE not in ('','{}') Then
                        (ifnull(concat(cast(date_format(e.ENTL_END_DATE,'%Y-%m-%d') as char),'T12:00:00.000Z'),''))
                        Else '' End As ENTL_END_DATE,
        ifnull(e.ASSET_OWNER_ADDRESS_ID,'') as ASSET_OWNER_ADDRESS_ID,
        concat(rf.AGREE_PO, ' ', 'Re-activation Fee') as AGREE_PO,
        ifnull(e.BUNDLE_PART_TRNS,'') as BUNDLE_PART_TRNS,
        ifnull(e.AGREE_RES_MASTER_ACCNT_ID,'') as AGREE_RES_MASTER_ACCNT_ID,
        Case When e.ENTL_ST_DATE not in ('','{}') Then
                        (ifnull(concat(cast(date_format(e.ENTL_ST_DATE,'%Y-%m-%d') as char),'T12:00:00.000Z'),''))
                        Else '' End As ENTL_ST_DATE,
        ifnull(e.ASSET_PRODUCT_DIVISION,'') as ASSET_PRODUCT_DIVISION,
        ifnull(e.ASSET_PRODUCT_LINE,'') as ASSET_PRODUCT_LINE,
        ifnull(e.ASSET_MASTER_PRODUCT_GROUP,'') as ASSET_MASTER_PRODUCT_GROUP,
        ifnull(replace(e.MSRP,'%','') ,'') as MSRP,
        case when e.EOSL in ('0','','Unspecified') then 'No'
        when e.EOSL IS NULL then 'No' else 'Yes' end EOSL,
        ifnull(e.AGREE_ID,'') as AGREE_ID,
        ifnull(e.NO_OF_ASSET_ENTL_ON_LINE,'') as NO_OF_ASSET_ENTL_ON_LINE,
        ifnull(e.AGREE_LINE_ID,'') as AGREE_LINE_ID,
        'Re-activation Fee',
        Case When e.ASSET_SHIP_DATE not in ('','{}') Then
                        (ifnull(concat(cast(date_format(e.ASSET_SHIP_DATE,'%Y-%m-%d') as char),'T12:00:00.000Z'),''))
                        Else '' End As ASSET_SHIP_DATE,
        ifnull(e.ASSET_SHIP_YEAR,'') as ASSET_SHIP_YEAR,
        ifnull(e.ASSET_PO_NUM,'') as ASSET_PO_NUM,
        ifnull(e.ASSET_SO_NUM,'') as ASSET_SO_NUM,
        ifnull(e.ASSET_PART_NUM,'') as ASSET_PART_NUM,
        ifnull(e.ASSET_PRODUCT_ID,'') as ASSET_PRODUCT_ID,
        Case When e.ENTL_CREATE_DATE not in ('','{}') Then
                        (ifnull(concat(cast(date_format(e.ENTL_CREATE_DATE,'%Y-%m-%d') as char),'T12:00:00.000Z'),''))
                        Else '' End As ENTL_CREATE_DATE,
        ifnull(e.ENTL_ID,'') as ENTL_ID,
        ifnull(e.ENTL_NAME,'') as ENTL_NAME,
        ifnull(e.AGREE_ACCNT_NAME,'') as AGREE_MSTR_ACCNT_NAME,
       case when e.AGREE_END_CUST_ID in ('0','','Unspecified') then '' else e.AGREE_END_CUST_ID end as AGREE_MASTER_END_CUST_ID,
       case when e.AGREE_END_CUST_NAME in ('0','','Unspecified') then '' else e.AGREE_END_CUST_NAME end as  END_USER_MASTER_ACCOUNT_NAME,
       case when e.AGREE_RES_ACCNT_NAME in ('0','Unspecified') then '' else e.AGREE_RES_ACCNT_NAME end 	As MASTER_ACCOUNT_RESELLER_NAME,
       case when e.ASSET_OWNER_ID in ('0','','Unspecified') then '' else e.ASSET_OWNER_ID end as ASSET_OWNER_ID,
        
        ifnull(e.AGREE_LINE_QTY,'') As AGREE_LINE_QTY,
        ifnull(e.ENTL_NET_PRICE_PER_ASSET,'') As ENTL_NET_PRICE_PER_ASSET,
        ifnull(e.DISCOUNT_CODE,'') As DISCOUNT_CODE,
        ifnull(e.AGREE_SO,'') As AGREE_SO,
        ifnull(e.SO_CURRENCY,'') As SO_CURRENCY,
        CASE
                WHEN  e.THEATER='APAC' THEN 'servicesource'
                WHEN  e.THEATER='EMEA' THEN 'polycom'
                ELSE ''
        END AS OWNERSHIP,
        ifnull(e.MKTG_NAME,'') As MKTG_NAME,
        Case When e.EOSL not in ('','{}') Then
                        (ifnull(concat(cast(date_format(e.EOSL,'%Y-%m-%d') as char),'T12:00:00.000Z'),''))
                        Else '' End As EOSL_DATE,
        e.AGREE_LINE_SERVICE_PART_NUM,
        Case When e.ENTL_END_DATE not in ('','{}') Then
                        (ifnull(concat(cast(date_format(e.ENTL_END_DATE,'%Y-%m-%d') as char),'T12:00:00.000Z'),''))
                        Else '' End As EXTERNAL_ENTL_END_DATE,
        Case When e.ENTL_CREATE_DATE not in ('','{}') Then
                        (ifnull(concat(cast(date_format(e.ENTL_CREATE_DATE,'%Y-%m-%d') as char),'T12:00:00.000Z'),''))
                        Else '' End As EXTERNAL_ENTL_CREATE_DATE,
        ifnull(e.ASSET_OWNER_ADDRESS_1,'') As ASSET_OWNER_ADDRESS_1,
        ifnull(replace(e.ASSET_OWNER_COUNTRY,'Unspecified',''),'') As ASSET_OWNER_COUNTRY,
        ifnull(replace(e.ASSET_OWNER_POSTAL,'Unspecified',''),'') As ASSET_OWNER_POSTAL,
        ifnull(Te.name,e.THEATER) As POLYCOM_TERRITORY,
        case when e.PORTAL_PRIMARY_AGREE_ID IN ('0','','Unspecified') then '' else ifnull(e.PORTAL_PRIMARY_AGREE_ID,'') end  As PORTAL_PRIMARY_AGREE_ID,
        ifnull(e.PIM_DESCRIPTION,'') As PIM_DESCRIPTION
FROM
        polycom_tmp.reactivation_fee rf
        INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY e ON rf.DE_ID = e.TMP_ID
        inner join polycom.Polycom_app_lookups T on T.DisplayName=e.THEATER  and T.GroupName='ClientTheatre'
        inner join polycom.Polycom_app_lookups Te on Te.DisplayName=e.THEATER and Te.GroupName='ClientTerritory'
        left join polycom.Polycom_app_lookups R on R.DisplayName=e.AGREE_SALES_REGION

INTO OUTFILE '${EX_DIR}/Polycom_ReactivationFee_SERVICE_ASSET_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 3.1.1' from DUAL;
/*
**	3.1.1 Find a most frequently used 'Close Reason' for the opportunity
*/
INSERT INTO polycom_tmp.close_sale_use_reason
SELECT DISTINCT
	tc0.OPP_ID,
	tc0.RESULT_REASON
FROM
	(SELECT OPP_ID, RESULT_REASON, COUNT(OPP_ID) R_COUNT
	FROM polycom_tmp.close_sale_result_reason
	GROUP BY OPP_ID, RESULT_REASON) as tc0
INNER JOIN
	(SELECT OPP_ID, MAX(R_COUNT) M_COUNT
	FROM
		(SELECT OPP_ID, RESULT_REASON, COUNT(OPP_ID) R_COUNT
		FROM polycom_tmp.close_sale_result_reason
		GROUP BY OPP_ID, RESULT_REASON) as tc2
	GROUP BY OPP_ID) as tc1
	ON tc1.OPP_ID = tc0.OPP_ID
	AND tc1.M_COUNT = tc0.R_COUNT;
commit;
/*
**====</Step 3>=====
*/

SELECT NOW(), ' Executing Step 4.0' from DUAL;
/*
**====<Step 4>=====
**	4.0.0 Output update the opportunity fields scrub.
**	No need for the UNION query as the previously UNIONed entitlement_daily_activity_tmp is used in INNER JOIN.
	Appendex 4
*/
SELECT
	'_id(string)',
	'extensions.tenant.polycomBookingDate(date)',
	'extensions.master.clientTheater.value.name(string)',
	'extensions.master.clientTerritory.value.name(string)',
	'extensions.master.clientRegion.value.name(string)'
UNION ALL
SELECT
	opp_e.OPP_ID,
	COALESCE(MIN(eda.SO_DATE),MIN(eda.ENTL_CREATE_DATE)),
	COALESCE(MIN(eda.THEATER), ''),
	COALESCE(MIN(eda.THEATER), ''),
	COALESCE(MIN(eda.AGREE_SALES_REGION), '')
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = t_offers.OFFID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.TMP_ID = edat.TMP_ID
WHERE
	opp_e.MATCH_STATUS in ('Full Match', 'Partial Match')
	AND opp_e.OPP_ID IS NOT NULL
GROUP BY
	opp_e.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_entitlement_resolve_as_win_update_opportunity_fields_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
/*
**	4.0.1 Output the 'ResolveAsWin' scrub
**	No need for the UNION query as the previously UNIONed entitlement_daily_activity_tmp is used in INNER JOIN.
	Appendex 4
*/
SELECT
	'Name',
	'resolutionDate',
	'poAmount',
	'poDate',
	'poNumber',
	'reason',
	'soAmount',
	'soDate',
	'soNumber'
FROM
	DUAL
UNION ALL
SELECT
	COALESCE(mso.LEADOPPID, ''),
	COALESCE(MIN(eda.SO_DATE),MIN(eda.ENTL_CREATE_DATE)),
	COALESCE(SUM(eda.ENTL_NET_PRICE_PER_ASSET), ''),
	COALESCE(MIN(eda.SO_DATE),MIN(eda.ENTL_CREATE_DATE)),
	IF(LENGTH(MIN(eda.AGREE_PO)) = 0, 'Blank', MIN(eda.AGREE_PO)),
	COALESCE(MIN(csur.RESULT_REASON), ''),
	COALESCE(SUM(eda.ENTL_NET_PRICE_PER_ASSET), ''),
	COALESCE(MIN(eda.SO_DATE),MIN(eda.ENTL_CREATE_DATE)),
	IF(LENGTH(MIN(eda.AGREE_SO)) = 0, 'Blank', MIN(eda.AGREE_SO))
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN polycom_tmp.close_sale_use_reason csur
	ON opp_e.OPP_ID = csur.OPP_ID
INNER JOIN ${SCHEMA}.T_MASTER_SUB_OPP mso
	ON mso.LEADOPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = mso.LEADOPPID
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = t_offers.OFFID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.TMP_ID = edat.TMP_ID
WHERE
	opp_e.MATCH_STATUS in ('Full Match', 'Partial Match')
GROUP BY
	opp_e.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_entitlement_resolve_as_win_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 4.1' from DUAL;
/*
**	4.1 Recalculate opportunities for 'Partial Match' Opportunity with underlying offers with 'matched' entitlements.
**	Appendix 8
*/
SELECT
	'_id(string)',
	'amount.amount(numeric)',
	'targetAmount.amount(numeric)',
	'extensions.tenant.priorRenewalAmountNotAnnualized.value.amount(numeric)',
	'extensions.tenant.listPrice.value.amount(numeric)',
	'targetDate(date)',
	'earliestNewStartDate(date)',
	'latestNewEndDate(date)'
FROM
	DUAL
UNION ALL
SELECT
	opp_e.OPP_ID,
	COALESCE(SUM(eda.AGREE_LINE_PART_NET_PER_ASSET), ''),
	COALESCE(SUM(offers.TARGETAMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_PRIORRENEWALAMOUNTNOTANNUALIZED_VALUE_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_LISTPRICE_AMOUNT), ''),
	COALESCE(MIN(offers.TARGETDATE), ''),
	COALESCE(MIN(eda.ENTL_ST_DATE), ''),
	COALESCE(MAX(eda.ENTL_END_DATE), '')
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = t_offers.OFFID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.TMP_ID = edat.TMP_ID
WHERE
	opp_e.MATCH_STATUS in ('Full Match', 'Partial Match')
	AND opp_e.OPP_ID IS NOT NULL
GROUP BY
	opp_e.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_entitlement_recalculate_split_opportunity_with_matched_offers_resolve_as_win_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 4.2' from DUAL;
/*
**	4.2 Recalculate opportunities for 'Partial Match' Opportunity with underlying offers with 'not matched' entitlements.
**	Appendix 8
**	This will be the split opportunity to get created in Renew.
*/
SELECT
	'_id(string)',
	'amount.amount(numeric)',
	'targetAmount.amount(numeric)',
	'extensions.tenant.priorRenewalAmountNotAnnualized.value.amount(numeric)',
	'extensions.tenant.listPrice.value.amount(numeric)',
	'targetDate(date)',
	'earliestNewStartDate(date)',
	'latestNewEndDate(date)'
FROM
	DUAL
UNION ALL
SELECT
	opp_e.OPP_ID,
	COALESCE(SUM(offers.AMOUNT_AMOUNT), ''), /* This has to come from offers because it is 'not matched' */
	COALESCE(SUM(offers.TARGETAMOUNT_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_PRIORRENEWALAMOUNTNOTANNUALIZED_VALUE_AMOUNT), ''),
	COALESCE(SUM(offers.EXTENSIONS_TENANT_LISTPRICE_AMOUNT), ''),
	COALESCE(MIN(offers.TARGETDATE), ''),
	COALESCE(MIN(offers.STARTDATE), ''), /* This has to come from offers because it is 'not matched' */
	COALESCE(MAX(offers.ENDDATE), '') /* This has to come from offers because it is 'not matched' */
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = offers._ID
	AND edat.MATCH_STATUS = 'Matched'
WHERE
	opp_e.MATCH_STATUS = 'Partial Match'
	AND edat.TMP_ID IS NULL
	AND opp_e.OPP_ID IS NOT NULL
GROUP BY
	opp_e.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_entitlement_recalculate_split_opportunity_with_unmatched_offers_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
/*
**	4.2.2 \"Resolve Back to Original Sales Stage\" reopened \"split\" opportunity.
**	Opportunity with underlying offers with 'not matched' entitlements.
**	This will be the split opportunity to get created in Renew.
*/
SELECT
	'_id(string)',
	'resolutionDate(date)',
	'resultReason(string)'
FROM
	DUAL
UNION ALL
SELECT
	opp_e.OPP_ID,
	COALESCE(opp.RESOLUTIONDATE, ''),
	COALESCE(MIN(offers.RESULTREASON_NAME), '')
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = offers._ID
	AND edat.MATCH_STATUS = 'Matched'
WHERE
	opp_e.MATCH_STATUS = 'Partial Match'
	AND opp_e.IS_REOPENED = true
	AND edat.TMP_ID IS NULL
	AND opp_e.OPP_ID IS NOT NULL
GROUP BY
	opp_e.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_entitlement_resolve_as_win_reset_back_reopened_opportunities_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

SELECT NOW(), ' Executing Step 4.3' from DUAL;
/*
**	4.3 Produce 'Split Opportunity' with offers that did not match.
**	This will be the split opportunity to get created in Renew.
*/
SELECT
	'Opportunity',
	'OfferIds'
FROM
	DUAL
UNION ALL
SELECT
	opp_e.OPP_ID,
	group_concat(offers._ID)
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_BASE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
LEFT OUTER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = offers._ID
	AND edat.MATCH_STATUS = 'Matched'
WHERE
	opp_e.MATCH_STATUS = 'Partial Match'
	AND edat.TMP_ID IS NULL
	AND opp_e.OPP_ID IS NOT NULL
	AND offers._ID IS NOT NULL
GROUP BY
	opp_e.OPP_ID

INTO OUTFILE '${EX_DIR}/polycom_entitlement_split_opportunity_with_unmatched_offers_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**====</Step 4>=====
*/

SELECT NOW(), ' Executing Step 5' from DUAL;
/*
**===<Step 5>
**	5.1 Produce the exception report to the client
**	'Unmatched' exception report
*/
SELECT
	'EXCEPTION', 'AGREE_ID', 'AGREE_NAME', 'AGREE_NUM', 'AGREE_PO', 'AGREE_SALES_REGION', 'THEATER', 'AGREE_SO', 'NO_OF_ASSET_ENTL_ON_LINE', 'AGREE_LINE_ID', 'AGREE_LINE_NUM', 'AGREE_LINE_PART_LIST', 'AGREE_LINE_PART_NET', 'AGREE_LINE_PART_NET_PER_ASSET', 'ANNUALIZED_VALUE', 'AGREE_LINE_QTY', 'AGREE_LINE_CURRENCY', 'MSRP', 'MKTG_NAME', 'DISCOUNT_CODE', 'BUNDLE_PART_TRNS', 'AGREE_LINE_SERVICE_PART_NUM', 'PIM_DESCRIPTION', 'ASSET_NUM', 'ASSET_ID', 'ASSET_PO_NUM', 'ASSET_SERIAL_NUM', 'ASSET_SERVICE_REGION', 'ASSET_SHIP_DATE', 'ASSET_SHIP_YEAR', 'ASSET_SO_NUM', 'ASSET_ADDRESS_1', 'ASSET_ADDRESS_2', 'ASSET_ADDRESS_ID', 'ASSET_CITY', 'ASSET_COUNTRY', 'ASSET_POSTAL', 'ASSET_STATE', 'ASSET_PART_NUM', 'ASSET_PRODUCT', 'ASSET_PRODUCT_DIVISION', 'ASSET_PRODUCT_GROUP', 'ASSET_MASTER_PRODUCT_GROUP', 'EOSL', 'ASSET_PRODUCT_ID', 'ASSET_PRODUCT_LINE', 'ENTL_CREATE_DATE', 'ENTL_DELIVERY_TYPE', 'ENTL_END_DATE', 'ENTL_BATCH_QUARTER', 'ENTL_ID', 'ENTL_NAME', 'ENTL_NET_PRICE_PER_ASSET', 'ENTL_NET_PRICE_PER_ASSET_USD', 'ENTL_SERVICE_TYPE', 'ENTL_ST_DATE', 'PREV_AGREE_ID', 'PREV_ENTL_END', 'PREV_ENTL_PO', 'PREV_ENTL_SRVC_PART_NUM', 'PREV_ENTITLEMENT_ID', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE', 'SO_EXT_NET_PRICE_USD', 'AGREE_ACCNT_ID', 'AGREE_ACCNT_NAME', 'AGREE_MASTER_ACCNT_ID', 'AGREE_MSTR_ACCNT_NAME', 'AGREE_ACCNT_GAN', 'AGREE_ACCNT_ADDRESS_1', 'AGREE_ACCNT_ADDRESS_2', 'AGREE_ACCNT_ADDRESS_ID', 'AGREE_ACCNT_CITY', 'AGREE_ACCNT_COUNTRY', 'AGREE_ACCNT_POSTAL', 'AGREE_ACCNT_STATE', 'AGREE_ACCNT_CONTACT_EMAIL', 'AGREE_ACCNT_CONTACT_NAME', 'AGREE_ACCNT_CONTACT_PHONE', 'AGREE_ACCNT_PGS_CONTACT_EMAIL', 'AGREE_ACCNT_PGS_CONTACT_FIRST', 'AGREE_ACCNT_PGS_CONTACT_ID', 'AGREE_ACCNT_PGS_CONTACT_LAST', 'AGREE_ACCNT_PGS_CONTACT_PHONE', 'AGREE_END_CUST_ID', 'AGREE_END_CUST_MSTR_ID', 'AGREE_END_CUST_NAME', 'AGREE_END_CUST_MSTR_NAME', 'AGREE_END_CUST_ADDRESS_1', 'AGREE_END_CUST_ADDRESS_2', 'AGREE_END_CUST_ADDRESS_ID', 'AGREE_END_CUST_CITY', 'AGREE_END_CUST_COUNTRY', 'AGREE_END_CUST_POSTAL', 'AGREE_END_CUST_STATE', 'AGREE_END_CUST_CONTACT_EMAIL', 'AGREE_END_CUST_CONTACT_NAME', 'AGREE_END_CUST_CONTACT_PHONE', 'AGREE_END_CUST_PGS_CONTACT_EMAIL', 'AGREE_END_CUST_PGS_CONTACT_FIRST', 'AGREE_END_CUST_PGS_CONTACT_ID', 'AGREE_END_CUST_PGS_CONTACT_LAST', 'AGREE_END_CUST_PGS_CONTACT_PHONE', 'AGREE_RES_MASTER_ACCNT_ID', 'AGREE_RES_ACCNT_NAME', 'AGREE_RES_ACCT_MSTR_NAME', 'AGREE_RES_ID', 'AGREE_RES_ADDRESS_1', 'AGREE_RES_ADDRESS_2', 'AGREE_RES_ADDRESS_ID', 'AGREE_RES_CITY', 'AGREE_RES_COUNTRY', 'AGREE_RES_POSTAL', 'AGREE_RES_STATE', 'AGREE_RES_CONTACT_EMAIL', 'AGREE_RES_CONTAC_NAME', 'AGREE_RES_CONTACT_PHONE', 'AGREE_RES_PGS_CONTACT_EMAIL', 'AGREE_RES_PGS_CONTACT_FIRST', 'AGREE_RES_PGS_CONTACT_ID', 'AGREE_RES_PGS_CONTACT_LAST', 'AGREE_RES_PGS_CONTACT_PHONE', 'AGREE_SHIP_ACCNT_NAME', 'AGREE_SHIP_ID', 'AGREE_SHIP_ADDRESS_1', 'AGREE_SHIP_ADDRESS_2', 'AGREE_SHIP_ADDRESS_ID', 'AGREE_SHIP_CITY', 'AGREE_SHIP_COUNTRY', 'AGREE_SHIP_POSTAL', 'AGREE_SHIP_STATE', 'AGREE_SHIP_CONTACT_EMAIL', 'AGREE_SHIP_CONTACT_NAME', 'AGREE_SHIP_CONTACT_PHONE', 'AGREE_SHIP_PGS_CONTACT_EMAIL', 'AGREE_SHIP_PGS_CONTACT_FIRST', 'AGREE_SHIP_PGS_CONTACT_ID', 'AGREE_SHIP_PGS_CONTACT_LAST', 'AGREE_SHIP_PGS_CONTACT_PHONE', 'ASSET_OWNER_ACCNT_NAME', 'ASSET_OWNER_ID', 'ASSET_OWNER_ADDRESS_1', 'ASSET_OWNER_ADDRESS_2', 'ASSET_OWNER_ADDRESS_ID', 'ASSET_OWNER_CITY', 'ASSET_OWNER_COUNTRY', 'ASSET_OWNER_POSTAL', 'ASSET_OWNER_STATE', 'ASSET_CONTACT_EMAIL', 'ASSET_CONTACT_FIRST', 'ASSET_CONTACT_ID', 'ASSET_CONTACT_LAST', 'ASSET_CONTACT_PHONE', 'ASSET_STATUS', 'POLYCOM_TERRITORY', 'PORTAL_PRIMARY_AGREE_NAME', 'PORTAL_PRIMARY_AGREE_ID'
FROM
	DUAL
UNION ALL
SELECT
	'Failed to Match',
	e.AGREE_ID, e.AGREE_NAME, e.AGREE_NUM, e.AGREE_PO, e.AGREE_SALES_REGION, e.THEATER, e.AGREE_SO, e.NO_OF_ASSET_ENTL_ON_LINE, e.AGREE_LINE_ID, e.AGREE_LINE_NUM, e.AGREE_LINE_PART_LIST, e.AGREE_LINE_PART_NET, e.AGREE_LINE_PART_NET_PER_ASSET, e.ANNUALIZED_VALUE, e.AGREE_LINE_QTY, e.AGREE_LINE_CURRENCY, e.MSRP, e.MKTG_NAME, e.DISCOUNT_CODE, e.BUNDLE_PART_TRNS, e.AGREE_LINE_SERVICE_PART_NUM, e.PIM_DESCRIPTION, e.ASSET_NUM, e.ASSET_ID, e.ASSET_PO_NUM, e.ASSET_SERIAL_NUM, e.ASSET_SERVICE_REGION, e.ASSET_SHIP_DATE, e.ASSET_SHIP_YEAR, e.ASSET_SO_NUM, e.ASSET_ADDRESS_1, e.ASSET_ADDRESS_2, e.ASSET_ADDRESS_ID, e.ASSET_CITY, e.ASSET_COUNTRY, e.ASSET_POSTAL, e.ASSET_STATE, e.ASSET_PART_NUM, e.ASSET_PRODUCT, e.ASSET_PRODUCT_DIVISION, e.ASSET_PRODUCT_GROUP, e.ASSET_MASTER_PRODUCT_GROUP, e.EOSL, e.ASSET_PRODUCT_ID, e.ASSET_PRODUCT_LINE, e.ENTL_CREATE_DATE, e.ENTL_DELIVERY_TYPE, e.ENTL_END_DATE, e.ENTL_BATCH_QUARTER, e.ENTL_ID, e.ENTL_NAME, e.ENTL_NET_PRICE_PER_ASSET, e.ENTL_NET_PRICE_PER_ASSET_USD, e.ENTL_SERVICE_TYPE, e.ENTL_ST_DATE, e.PREV_AGREE_ID, e.PREV_ENTL_END, e.PREV_ENTL_PO, e.PREV_ENTL_SRVC_PART_NUM, e.PREV_ENTITLEMENT_ID, e.SO_CURRENCY, e.SO_DATE, e.SO_EXT_NET_PRICE, e.SO_EXT_NET_PRICE_USD, e.AGREE_ACCNT_ID, e.AGREE_ACCNT_NAME, e.AGREE_MASTER_ACCNT_ID, e.AGREE_MSTR_ACCNT_NAME, e.AGREE_ACCNT_GAN, e.AGREE_ACCNT_ADDRESS_1, e.AGREE_ACCNT_ADDRESS_2, e.AGREE_ACCNT_ADDRESS_ID, e.AGREE_ACCNT_CITY, e.AGREE_ACCNT_COUNTRY, e.AGREE_ACCNT_POSTAL, e.AGREE_ACCNT_STATE, e.AGREE_ACCNT_CONTACT_EMAIL, e.AGREE_ACCNT_CONTACT_NAME, e.AGREE_ACCNT_CONTACT_PHONE, e.AGREE_ACCNT_PGS_CONTACT_EMAIL, e.AGREE_ACCNT_PGS_CONTACT_FIRST, e.AGREE_ACCNT_PGS_CONTACT_ID, e.AGREE_ACCNT_PGS_CONTACT_LAST, e.AGREE_ACCNT_PGS_CONTACT_PHONE, e.AGREE_END_CUST_ID, e.AGREE_END_CUST_MSTR_ID, e.AGREE_END_CUST_NAME, e.AGREE_END_CUST_MSTR_NAME, e.AGREE_END_CUST_ADDRESS_1, e.AGREE_END_CUST_ADDRESS_2, e.AGREE_END_CUST_ADDRESS_ID, e.AGREE_END_CUST_CITY, e.AGREE_END_CUST_COUNTRY, e.AGREE_END_CUST_POSTAL, e.AGREE_END_CUST_STATE, e.AGREE_END_CUST_CONTACT_EMAIL, e.AGREE_END_CUST_CONTACT_NAME, e.AGREE_END_CUST_CONTACT_PHONE, e.AGREE_END_CUST_PGS_CONTACT_EMAIL, e.AGREE_END_CUST_PGS_CONTACT_FIRST, e.AGREE_END_CUST_PGS_CONTACT_ID, e.AGREE_END_CUST_PGS_CONTACT_LAST, e.AGREE_END_CUST_PGS_CONTACT_PHONE, e.AGREE_RES_MASTER_ACCNT_ID, e.AGREE_RES_ACCNT_NAME, e.AGREE_RES_ACCT_MSTR_NAME, e.AGREE_RES_ID, e.AGREE_RES_ADDRESS_1, e.AGREE_RES_ADDRESS_2, e.AGREE_RES_ADDRESS_ID, e.AGREE_RES_CITY, e.AGREE_RES_COUNTRY, e.AGREE_RES_POSTAL, e.AGREE_RES_STATE, e.AGREE_RES_CONTACT_EMAIL, e.AGREE_RES_CONTAC_NAME, e.AGREE_RES_CONTACT_PHONE, e.AGREE_RES_PGS_CONTACT_EMAIL, e.AGREE_RES_PGS_CONTACT_FIRST, e.AGREE_RES_PGS_CONTACT_ID, e.AGREE_RES_PGS_CONTACT_LAST, e.AGREE_RES_PGS_CONTACT_PHONE, e.AGREE_SHIP_ACCNT_NAME, e.AGREE_SHIP_ID, e.AGREE_SHIP_ADDRESS_1, e.AGREE_SHIP_ADDRESS_2, e.AGREE_SHIP_ADDRESS_ID, e.AGREE_SHIP_CITY, e.AGREE_SHIP_COUNTRY, e.AGREE_SHIP_POSTAL, e.AGREE_SHIP_STATE, e.AGREE_SHIP_CONTACT_EMAIL, e.AGREE_SHIP_CONTACT_NAME, e.AGREE_SHIP_CONTACT_PHONE, e.AGREE_SHIP_PGS_CONTACT_EMAIL, e.AGREE_SHIP_PGS_CONTACT_FIRST, e.AGREE_SHIP_PGS_CONTACT_ID, e.AGREE_SHIP_PGS_CONTACT_LAST, e.AGREE_SHIP_PGS_CONTACT_PHONE, e.ASSET_OWNER_ACCNT_NAME, e.ASSET_OWNER_ID, e.ASSET_OWNER_ADDRESS_1, e.ASSET_OWNER_ADDRESS_2, e.ASSET_OWNER_ADDRESS_ID, e.ASSET_OWNER_CITY, e.ASSET_OWNER_COUNTRY, e.ASSET_OWNER_POSTAL, e.ASSET_OWNER_STATE, e.ASSET_CONTACT_EMAIL, e.ASSET_CONTACT_FIRST, e.ASSET_CONTACT_ID, e.ASSET_CONTACT_LAST, e.ASSET_CONTACT_PHONE, e.ASSET_STATUS, e.POLYCOM_TERRITORY, e.PORTAL_PRIMARY_AGREE_NAME, e.PORTAL_PRIMARY_AGREE_ID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY e
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.TMP_ID = e.TMP_ID
	AND edat.MATCH_STATUS = 'Not Matched'
WHERE
	e.SS_STATUS = 'Not Processed'
	${ENTITLEMENT_EXCEPTION_DATE_CLAUSE}

INTO OUTFILE '${EX_DIR}/polycom_entitlement_exception_unmatched_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	5.2 Matched but already 'Closed Sale' exception report
*/
SELECT
	'EXCEPTION', 'AGREE_ID', 'AGREE_NAME', 'AGREE_NUM', 'AGREE_PO', 'AGREE_SALES_REGION', 'THEATER', 'AGREE_SO', 'NO_OF_ASSET_ENTL_ON_LINE', 'AGREE_LINE_ID', 'AGREE_LINE_NUM', 'AGREE_LINE_PART_LIST', 'AGREE_LINE_PART_NET', 'AGREE_LINE_PART_NET_PER_ASSET', 'ANNUALIZED_VALUE', 'AGREE_LINE_QTY', 'AGREE_LINE_CURRENCY', 'MSRP', 'MKTG_NAME', 'DISCOUNT_CODE', 'BUNDLE_PART_TRNS', 'AGREE_LINE_SERVICE_PART_NUM', 'PIM_DESCRIPTION', 'ASSET_NUM', 'ASSET_ID', 'ASSET_PO_NUM', 'ASSET_SERIAL_NUM', 'ASSET_SERVICE_REGION', 'ASSET_SHIP_DATE', 'ASSET_SHIP_YEAR', 'ASSET_SO_NUM', 'ASSET_ADDRESS_1', 'ASSET_ADDRESS_2', 'ASSET_ADDRESS_ID', 'ASSET_CITY', 'ASSET_COUNTRY', 'ASSET_POSTAL', 'ASSET_STATE', 'ASSET_PART_NUM', 'ASSET_PRODUCT', 'ASSET_PRODUCT_DIVISION', 'ASSET_PRODUCT_GROUP', 'ASSET_MASTER_PRODUCT_GROUP', 'EOSL', 'ASSET_PRODUCT_ID', 'ASSET_PRODUCT_LINE', 'ENTL_CREATE_DATE', 'ENTL_DELIVERY_TYPE', 'ENTL_END_DATE', 'ENTL_BATCH_QUARTER', 'ENTL_ID', 'ENTL_NAME', 'ENTL_NET_PRICE_PER_ASSET', 'ENTL_NET_PRICE_PER_ASSET_USD', 'ENTL_SERVICE_TYPE', 'ENTL_ST_DATE', 'PREV_AGREE_ID', 'PREV_ENTL_END', 'PREV_ENTL_PO', 'PREV_ENTL_SRVC_PART_NUM', 'PREV_ENTITLEMENT_ID', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE', 'SO_EXT_NET_PRICE_USD', 'AGREE_ACCNT_ID', 'AGREE_ACCNT_NAME', 'AGREE_MASTER_ACCNT_ID', 'AGREE_MSTR_ACCNT_NAME', 'AGREE_ACCNT_GAN', 'AGREE_ACCNT_ADDRESS_1', 'AGREE_ACCNT_ADDRESS_2', 'AGREE_ACCNT_ADDRESS_ID', 'AGREE_ACCNT_CITY', 'AGREE_ACCNT_COUNTRY', 'AGREE_ACCNT_POSTAL', 'AGREE_ACCNT_STATE', 'AGREE_ACCNT_CONTACT_EMAIL', 'AGREE_ACCNT_CONTACT_NAME', 'AGREE_ACCNT_CONTACT_PHONE', 'AGREE_ACCNT_PGS_CONTACT_EMAIL', 'AGREE_ACCNT_PGS_CONTACT_FIRST', 'AGREE_ACCNT_PGS_CONTACT_ID', 'AGREE_ACCNT_PGS_CONTACT_LAST', 'AGREE_ACCNT_PGS_CONTACT_PHONE', 'AGREE_END_CUST_ID', 'AGREE_END_CUST_MSTR_ID', 'AGREE_END_CUST_NAME', 'AGREE_END_CUST_MSTR_NAME', 'AGREE_END_CUST_ADDRESS_1', 'AGREE_END_CUST_ADDRESS_2', 'AGREE_END_CUST_ADDRESS_ID', 'AGREE_END_CUST_CITY', 'AGREE_END_CUST_COUNTRY', 'AGREE_END_CUST_POSTAL', 'AGREE_END_CUST_STATE', 'AGREE_END_CUST_CONTACT_EMAIL', 'AGREE_END_CUST_CONTACT_NAME', 'AGREE_END_CUST_CONTACT_PHONE', 'AGREE_END_CUST_PGS_CONTACT_EMAIL', 'AGREE_END_CUST_PGS_CONTACT_FIRST', 'AGREE_END_CUST_PGS_CONTACT_ID', 'AGREE_END_CUST_PGS_CONTACT_LAST', 'AGREE_END_CUST_PGS_CONTACT_PHONE', 'AGREE_RES_MASTER_ACCNT_ID', 'AGREE_RES_ACCNT_NAME', 'AGREE_RES_ACCT_MSTR_NAME', 'AGREE_RES_ID', 'AGREE_RES_ADDRESS_1', 'AGREE_RES_ADDRESS_2', 'AGREE_RES_ADDRESS_ID', 'AGREE_RES_CITY', 'AGREE_RES_COUNTRY', 'AGREE_RES_POSTAL', 'AGREE_RES_STATE', 'AGREE_RES_CONTACT_EMAIL', 'AGREE_RES_CONTAC_NAME', 'AGREE_RES_CONTACT_PHONE', 'AGREE_RES_PGS_CONTACT_EMAIL', 'AGREE_RES_PGS_CONTACT_FIRST', 'AGREE_RES_PGS_CONTACT_ID', 'AGREE_RES_PGS_CONTACT_LAST', 'AGREE_RES_PGS_CONTACT_PHONE', 'AGREE_SHIP_ACCNT_NAME', 'AGREE_SHIP_ID', 'AGREE_SHIP_ADDRESS_1', 'AGREE_SHIP_ADDRESS_2', 'AGREE_SHIP_ADDRESS_ID', 'AGREE_SHIP_CITY', 'AGREE_SHIP_COUNTRY', 'AGREE_SHIP_POSTAL', 'AGREE_SHIP_STATE', 'AGREE_SHIP_CONTACT_EMAIL', 'AGREE_SHIP_CONTACT_NAME', 'AGREE_SHIP_CONTACT_PHONE', 'AGREE_SHIP_PGS_CONTACT_EMAIL', 'AGREE_SHIP_PGS_CONTACT_FIRST', 'AGREE_SHIP_PGS_CONTACT_ID', 'AGREE_SHIP_PGS_CONTACT_LAST', 'AGREE_SHIP_PGS_CONTACT_PHONE', 'ASSET_OWNER_ACCNT_NAME', 'ASSET_OWNER_ID', 'ASSET_OWNER_ADDRESS_1', 'ASSET_OWNER_ADDRESS_2', 'ASSET_OWNER_ADDRESS_ID', 'ASSET_OWNER_CITY', 'ASSET_OWNER_COUNTRY', 'ASSET_OWNER_POSTAL', 'ASSET_OWNER_STATE', 'ASSET_CONTACT_EMAIL', 'ASSET_CONTACT_FIRST', 'ASSET_CONTACT_ID', 'ASSET_CONTACT_LAST', 'ASSET_CONTACT_PHONE', 'ASSET_STATUS', 'POLYCOM_TERRITORY', 'PORTAL_PRIMARY_AGREE_NAME', 'PORTAL_PRIMARY_AGREE_ID'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	'Matched, but already Closed Sale',
	e.AGREE_ID, e.AGREE_NAME, e.AGREE_NUM, e.AGREE_PO, e.AGREE_SALES_REGION, e.THEATER, e.AGREE_SO, e.NO_OF_ASSET_ENTL_ON_LINE, e.AGREE_LINE_ID, e.AGREE_LINE_NUM, e.AGREE_LINE_PART_LIST, e.AGREE_LINE_PART_NET, e.AGREE_LINE_PART_NET_PER_ASSET, e.ANNUALIZED_VALUE, e.AGREE_LINE_QTY, e.AGREE_LINE_CURRENCY, e.MSRP, e.MKTG_NAME, e.DISCOUNT_CODE, e.BUNDLE_PART_TRNS, e.AGREE_LINE_SERVICE_PART_NUM, e.PIM_DESCRIPTION, e.ASSET_NUM, e.ASSET_ID, e.ASSET_PO_NUM, e.ASSET_SERIAL_NUM, e.ASSET_SERVICE_REGION, e.ASSET_SHIP_DATE, e.ASSET_SHIP_YEAR, e.ASSET_SO_NUM, e.ASSET_ADDRESS_1, e.ASSET_ADDRESS_2, e.ASSET_ADDRESS_ID, e.ASSET_CITY, e.ASSET_COUNTRY, e.ASSET_POSTAL, e.ASSET_STATE, e.ASSET_PART_NUM, e.ASSET_PRODUCT, e.ASSET_PRODUCT_DIVISION, e.ASSET_PRODUCT_GROUP, e.ASSET_MASTER_PRODUCT_GROUP, e.EOSL, e.ASSET_PRODUCT_ID, e.ASSET_PRODUCT_LINE, e.ENTL_CREATE_DATE, e.ENTL_DELIVERY_TYPE, e.ENTL_END_DATE, e.ENTL_BATCH_QUARTER, e.ENTL_ID, e.ENTL_NAME, e.ENTL_NET_PRICE_PER_ASSET, e.ENTL_NET_PRICE_PER_ASSET_USD, e.ENTL_SERVICE_TYPE, e.ENTL_ST_DATE, e.PREV_AGREE_ID, e.PREV_ENTL_END, e.PREV_ENTL_PO, e.PREV_ENTL_SRVC_PART_NUM, e.PREV_ENTITLEMENT_ID, e.SO_CURRENCY, e.SO_DATE, e.SO_EXT_NET_PRICE, e.SO_EXT_NET_PRICE_USD, e.AGREE_ACCNT_ID, e.AGREE_ACCNT_NAME, e.AGREE_MASTER_ACCNT_ID, e.AGREE_MSTR_ACCNT_NAME, e.AGREE_ACCNT_GAN, e.AGREE_ACCNT_ADDRESS_1, e.AGREE_ACCNT_ADDRESS_2, e.AGREE_ACCNT_ADDRESS_ID, e.AGREE_ACCNT_CITY, e.AGREE_ACCNT_COUNTRY, e.AGREE_ACCNT_POSTAL, e.AGREE_ACCNT_STATE, e.AGREE_ACCNT_CONTACT_EMAIL, e.AGREE_ACCNT_CONTACT_NAME, e.AGREE_ACCNT_CONTACT_PHONE, e.AGREE_ACCNT_PGS_CONTACT_EMAIL, e.AGREE_ACCNT_PGS_CONTACT_FIRST, e.AGREE_ACCNT_PGS_CONTACT_ID, e.AGREE_ACCNT_PGS_CONTACT_LAST, e.AGREE_ACCNT_PGS_CONTACT_PHONE, e.AGREE_END_CUST_ID, e.AGREE_END_CUST_MSTR_ID, e.AGREE_END_CUST_NAME, e.AGREE_END_CUST_MSTR_NAME, e.AGREE_END_CUST_ADDRESS_1, e.AGREE_END_CUST_ADDRESS_2, e.AGREE_END_CUST_ADDRESS_ID, e.AGREE_END_CUST_CITY, e.AGREE_END_CUST_COUNTRY, e.AGREE_END_CUST_POSTAL, e.AGREE_END_CUST_STATE, e.AGREE_END_CUST_CONTACT_EMAIL, e.AGREE_END_CUST_CONTACT_NAME, e.AGREE_END_CUST_CONTACT_PHONE, e.AGREE_END_CUST_PGS_CONTACT_EMAIL, e.AGREE_END_CUST_PGS_CONTACT_FIRST, e.AGREE_END_CUST_PGS_CONTACT_ID, e.AGREE_END_CUST_PGS_CONTACT_LAST, e.AGREE_END_CUST_PGS_CONTACT_PHONE, e.AGREE_RES_MASTER_ACCNT_ID, e.AGREE_RES_ACCNT_NAME, e.AGREE_RES_ACCT_MSTR_NAME, e.AGREE_RES_ID, e.AGREE_RES_ADDRESS_1, e.AGREE_RES_ADDRESS_2, e.AGREE_RES_ADDRESS_ID, e.AGREE_RES_CITY, e.AGREE_RES_COUNTRY, e.AGREE_RES_POSTAL, e.AGREE_RES_STATE, e.AGREE_RES_CONTACT_EMAIL, e.AGREE_RES_CONTAC_NAME, e.AGREE_RES_CONTACT_PHONE, e.AGREE_RES_PGS_CONTACT_EMAIL, e.AGREE_RES_PGS_CONTACT_FIRST, e.AGREE_RES_PGS_CONTACT_ID, e.AGREE_RES_PGS_CONTACT_LAST, e.AGREE_RES_PGS_CONTACT_PHONE, e.AGREE_SHIP_ACCNT_NAME, e.AGREE_SHIP_ID, e.AGREE_SHIP_ADDRESS_1, e.AGREE_SHIP_ADDRESS_2, e.AGREE_SHIP_ADDRESS_ID, e.AGREE_SHIP_CITY, e.AGREE_SHIP_COUNTRY, e.AGREE_SHIP_POSTAL, e.AGREE_SHIP_STATE, e.AGREE_SHIP_CONTACT_EMAIL, e.AGREE_SHIP_CONTACT_NAME, e.AGREE_SHIP_CONTACT_PHONE, e.AGREE_SHIP_PGS_CONTACT_EMAIL, e.AGREE_SHIP_PGS_CONTACT_FIRST, e.AGREE_SHIP_PGS_CONTACT_ID, e.AGREE_SHIP_PGS_CONTACT_LAST, e.AGREE_SHIP_PGS_CONTACT_PHONE, e.ASSET_OWNER_ACCNT_NAME, e.ASSET_OWNER_ID, e.ASSET_OWNER_ADDRESS_1, e.ASSET_OWNER_ADDRESS_2, e.ASSET_OWNER_ADDRESS_ID, e.ASSET_OWNER_CITY, e.ASSET_OWNER_COUNTRY, e.ASSET_OWNER_POSTAL, e.ASSET_OWNER_STATE, e.ASSET_CONTACT_EMAIL, e.ASSET_CONTACT_FIRST, e.ASSET_CONTACT_ID, e.ASSET_CONTACT_LAST, e.ASSET_CONTACT_PHONE, e.ASSET_STATUS, e.POLYCOM_TERRITORY, e.PORTAL_PRIMARY_AGREE_NAME, e.PORTAL_PRIMARY_AGREE_ID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY e
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON e.TMP_ID = edat.TMP_ID
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OFFID = edat.OFFER_ID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
WHERE
	opp.FLOWS_SALESSTAGES_STATE_NAME = 'closedSale'
	AND e.SS_STATUS = 'Not Processed'

INTO OUTFILE '${EX_DIR}/polycom_entitlement_exception_matched_but_closed_sale_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
/*
**	5.3 Matched but 'No Resolution Date' exception report
*/
SELECT
	'EXCEPTION', 'AGREE_ID', 'AGREE_NAME', 'AGREE_NUM', 'AGREE_PO', 'AGREE_SALES_REGION', 'THEATER', 'AGREE_SO', 'NO_OF_ASSET_ENTL_ON_LINE', 'AGREE_LINE_ID', 'AGREE_LINE_NUM', 'AGREE_LINE_PART_LIST', 'AGREE_LINE_PART_NET', 'AGREE_LINE_PART_NET_PER_ASSET', 'ANNUALIZED_VALUE', 'AGREE_LINE_QTY', 'AGREE_LINE_CURRENCY', 'MSRP', 'MKTG_NAME', 'DISCOUNT_CODE', 'BUNDLE_PART_TRNS', 'AGREE_LINE_SERVICE_PART_NUM', 'PIM_DESCRIPTION', 'ASSET_NUM', 'ASSET_ID', 'ASSET_PO_NUM', 'ASSET_SERIAL_NUM', 'ASSET_SERVICE_REGION', 'ASSET_SHIP_DATE', 'ASSET_SHIP_YEAR', 'ASSET_SO_NUM', 'ASSET_ADDRESS_1', 'ASSET_ADDRESS_2', 'ASSET_ADDRESS_ID', 'ASSET_CITY', 'ASSET_COUNTRY', 'ASSET_POSTAL', 'ASSET_STATE', 'ASSET_PART_NUM', 'ASSET_PRODUCT', 'ASSET_PRODUCT_DIVISION', 'ASSET_PRODUCT_GROUP', 'ASSET_MASTER_PRODUCT_GROUP', 'EOSL', 'ASSET_PRODUCT_ID', 'ASSET_PRODUCT_LINE', 'ENTL_CREATE_DATE', 'ENTL_DELIVERY_TYPE', 'ENTL_END_DATE', 'ENTL_BATCH_QUARTER', 'ENTL_ID', 'ENTL_NAME', 'ENTL_NET_PRICE_PER_ASSET', 'ENTL_NET_PRICE_PER_ASSET_USD', 'ENTL_SERVICE_TYPE', 'ENTL_ST_DATE', 'PREV_AGREE_ID', 'PREV_ENTL_END', 'PREV_ENTL_PO', 'PREV_ENTL_SRVC_PART_NUM', 'PREV_ENTITLEMENT_ID', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE', 'SO_EXT_NET_PRICE_USD', 'AGREE_ACCNT_ID', 'AGREE_ACCNT_NAME', 'AGREE_MASTER_ACCNT_ID', 'AGREE_MSTR_ACCNT_NAME', 'AGREE_ACCNT_GAN', 'AGREE_ACCNT_ADDRESS_1', 'AGREE_ACCNT_ADDRESS_2', 'AGREE_ACCNT_ADDRESS_ID', 'AGREE_ACCNT_CITY', 'AGREE_ACCNT_COUNTRY', 'AGREE_ACCNT_POSTAL', 'AGREE_ACCNT_STATE', 'AGREE_ACCNT_CONTACT_EMAIL', 'AGREE_ACCNT_CONTACT_NAME', 'AGREE_ACCNT_CONTACT_PHONE', 'AGREE_ACCNT_PGS_CONTACT_EMAIL', 'AGREE_ACCNT_PGS_CONTACT_FIRST', 'AGREE_ACCNT_PGS_CONTACT_ID', 'AGREE_ACCNT_PGS_CONTACT_LAST', 'AGREE_ACCNT_PGS_CONTACT_PHONE', 'AGREE_END_CUST_ID', 'AGREE_END_CUST_MSTR_ID', 'AGREE_END_CUST_NAME', 'AGREE_END_CUST_MSTR_NAME', 'AGREE_END_CUST_ADDRESS_1', 'AGREE_END_CUST_ADDRESS_2', 'AGREE_END_CUST_ADDRESS_ID', 'AGREE_END_CUST_CITY', 'AGREE_END_CUST_COUNTRY', 'AGREE_END_CUST_POSTAL', 'AGREE_END_CUST_STATE', 'AGREE_END_CUST_CONTACT_EMAIL', 'AGREE_END_CUST_CONTACT_NAME', 'AGREE_END_CUST_CONTACT_PHONE', 'AGREE_END_CUST_PGS_CONTACT_EMAIL', 'AGREE_END_CUST_PGS_CONTACT_FIRST', 'AGREE_END_CUST_PGS_CONTACT_ID', 'AGREE_END_CUST_PGS_CONTACT_LAST', 'AGREE_END_CUST_PGS_CONTACT_PHONE', 'AGREE_RES_MASTER_ACCNT_ID', 'AGREE_RES_ACCNT_NAME', 'AGREE_RES_ACCT_MSTR_NAME', 'AGREE_RES_ID', 'AGREE_RES_ADDRESS_1', 'AGREE_RES_ADDRESS_2', 'AGREE_RES_ADDRESS_ID', 'AGREE_RES_CITY', 'AGREE_RES_COUNTRY', 'AGREE_RES_POSTAL', 'AGREE_RES_STATE', 'AGREE_RES_CONTACT_EMAIL', 'AGREE_RES_CONTAC_NAME', 'AGREE_RES_CONTACT_PHONE', 'AGREE_RES_PGS_CONTACT_EMAIL', 'AGREE_RES_PGS_CONTACT_FIRST', 'AGREE_RES_PGS_CONTACT_ID', 'AGREE_RES_PGS_CONTACT_LAST', 'AGREE_RES_PGS_CONTACT_PHONE', 'AGREE_SHIP_ACCNT_NAME', 'AGREE_SHIP_ID', 'AGREE_SHIP_ADDRESS_1', 'AGREE_SHIP_ADDRESS_2', 'AGREE_SHIP_ADDRESS_ID', 'AGREE_SHIP_CITY', 'AGREE_SHIP_COUNTRY', 'AGREE_SHIP_POSTAL', 'AGREE_SHIP_STATE', 'AGREE_SHIP_CONTACT_EMAIL', 'AGREE_SHIP_CONTACT_NAME', 'AGREE_SHIP_CONTACT_PHONE', 'AGREE_SHIP_PGS_CONTACT_EMAIL', 'AGREE_SHIP_PGS_CONTACT_FIRST', 'AGREE_SHIP_PGS_CONTACT_ID', 'AGREE_SHIP_PGS_CONTACT_LAST', 'AGREE_SHIP_PGS_CONTACT_PHONE', 'ASSET_OWNER_ACCNT_NAME', 'ASSET_OWNER_ID', 'ASSET_OWNER_ADDRESS_1', 'ASSET_OWNER_ADDRESS_2', 'ASSET_OWNER_ADDRESS_ID', 'ASSET_OWNER_CITY', 'ASSET_OWNER_COUNTRY', 'ASSET_OWNER_POSTAL', 'ASSET_OWNER_STATE', 'ASSET_CONTACT_EMAIL', 'ASSET_CONTACT_FIRST', 'ASSET_CONTACT_ID', 'ASSET_CONTACT_LAST', 'ASSET_CONTACT_PHONE', 'ASSET_STATUS', 'POLYCOM_TERRITORY', 'PORTAL_PRIMARY_AGREE_NAME', 'PORTAL_PRIMARY_AGREE_ID'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	'Matched, but no Resolution Date',
	e.AGREE_ID, e.AGREE_NAME, e.AGREE_NUM, e.AGREE_PO, e.AGREE_SALES_REGION, e.THEATER, e.AGREE_SO, e.NO_OF_ASSET_ENTL_ON_LINE, e.AGREE_LINE_ID, e.AGREE_LINE_NUM, e.AGREE_LINE_PART_LIST, e.AGREE_LINE_PART_NET, e.AGREE_LINE_PART_NET_PER_ASSET, e.ANNUALIZED_VALUE, e.AGREE_LINE_QTY, e.AGREE_LINE_CURRENCY, e.MSRP, e.MKTG_NAME, e.DISCOUNT_CODE, e.BUNDLE_PART_TRNS, e.AGREE_LINE_SERVICE_PART_NUM, e.PIM_DESCRIPTION, e.ASSET_NUM, e.ASSET_ID, e.ASSET_PO_NUM, e.ASSET_SERIAL_NUM, e.ASSET_SERVICE_REGION, e.ASSET_SHIP_DATE, e.ASSET_SHIP_YEAR, e.ASSET_SO_NUM, e.ASSET_ADDRESS_1, e.ASSET_ADDRESS_2, e.ASSET_ADDRESS_ID, e.ASSET_CITY, e.ASSET_COUNTRY, e.ASSET_POSTAL, e.ASSET_STATE, e.ASSET_PART_NUM, e.ASSET_PRODUCT, e.ASSET_PRODUCT_DIVISION, e.ASSET_PRODUCT_GROUP, e.ASSET_MASTER_PRODUCT_GROUP, e.EOSL, e.ASSET_PRODUCT_ID, e.ASSET_PRODUCT_LINE, e.ENTL_CREATE_DATE, e.ENTL_DELIVERY_TYPE, e.ENTL_END_DATE, e.ENTL_BATCH_QUARTER, e.ENTL_ID, e.ENTL_NAME, e.ENTL_NET_PRICE_PER_ASSET, e.ENTL_NET_PRICE_PER_ASSET_USD, e.ENTL_SERVICE_TYPE, e.ENTL_ST_DATE, e.PREV_AGREE_ID, e.PREV_ENTL_END, e.PREV_ENTL_PO, e.PREV_ENTL_SRVC_PART_NUM, e.PREV_ENTITLEMENT_ID, e.SO_CURRENCY, e.SO_DATE, e.SO_EXT_NET_PRICE, e.SO_EXT_NET_PRICE_USD, e.AGREE_ACCNT_ID, e.AGREE_ACCNT_NAME, e.AGREE_MASTER_ACCNT_ID, e.AGREE_MSTR_ACCNT_NAME, e.AGREE_ACCNT_GAN, e.AGREE_ACCNT_ADDRESS_1, e.AGREE_ACCNT_ADDRESS_2, e.AGREE_ACCNT_ADDRESS_ID, e.AGREE_ACCNT_CITY, e.AGREE_ACCNT_COUNTRY, e.AGREE_ACCNT_POSTAL, e.AGREE_ACCNT_STATE, e.AGREE_ACCNT_CONTACT_EMAIL, e.AGREE_ACCNT_CONTACT_NAME, e.AGREE_ACCNT_CONTACT_PHONE, e.AGREE_ACCNT_PGS_CONTACT_EMAIL, e.AGREE_ACCNT_PGS_CONTACT_FIRST, e.AGREE_ACCNT_PGS_CONTACT_ID, e.AGREE_ACCNT_PGS_CONTACT_LAST, e.AGREE_ACCNT_PGS_CONTACT_PHONE, e.AGREE_END_CUST_ID, e.AGREE_END_CUST_MSTR_ID, e.AGREE_END_CUST_NAME, e.AGREE_END_CUST_MSTR_NAME, e.AGREE_END_CUST_ADDRESS_1, e.AGREE_END_CUST_ADDRESS_2, e.AGREE_END_CUST_ADDRESS_ID, e.AGREE_END_CUST_CITY, e.AGREE_END_CUST_COUNTRY, e.AGREE_END_CUST_POSTAL, e.AGREE_END_CUST_STATE, e.AGREE_END_CUST_CONTACT_EMAIL, e.AGREE_END_CUST_CONTACT_NAME, e.AGREE_END_CUST_CONTACT_PHONE, e.AGREE_END_CUST_PGS_CONTACT_EMAIL, e.AGREE_END_CUST_PGS_CONTACT_FIRST, e.AGREE_END_CUST_PGS_CONTACT_ID, e.AGREE_END_CUST_PGS_CONTACT_LAST, e.AGREE_END_CUST_PGS_CONTACT_PHONE, e.AGREE_RES_MASTER_ACCNT_ID, e.AGREE_RES_ACCNT_NAME, e.AGREE_RES_ACCT_MSTR_NAME, e.AGREE_RES_ID, e.AGREE_RES_ADDRESS_1, e.AGREE_RES_ADDRESS_2, e.AGREE_RES_ADDRESS_ID, e.AGREE_RES_CITY, e.AGREE_RES_COUNTRY, e.AGREE_RES_POSTAL, e.AGREE_RES_STATE, e.AGREE_RES_CONTACT_EMAIL, e.AGREE_RES_CONTAC_NAME, e.AGREE_RES_CONTACT_PHONE, e.AGREE_RES_PGS_CONTACT_EMAIL, e.AGREE_RES_PGS_CONTACT_FIRST, e.AGREE_RES_PGS_CONTACT_ID, e.AGREE_RES_PGS_CONTACT_LAST, e.AGREE_RES_PGS_CONTACT_PHONE, e.AGREE_SHIP_ACCNT_NAME, e.AGREE_SHIP_ID, e.AGREE_SHIP_ADDRESS_1, e.AGREE_SHIP_ADDRESS_2, e.AGREE_SHIP_ADDRESS_ID, e.AGREE_SHIP_CITY, e.AGREE_SHIP_COUNTRY, e.AGREE_SHIP_POSTAL, e.AGREE_SHIP_STATE, e.AGREE_SHIP_CONTACT_EMAIL, e.AGREE_SHIP_CONTACT_NAME, e.AGREE_SHIP_CONTACT_PHONE, e.AGREE_SHIP_PGS_CONTACT_EMAIL, e.AGREE_SHIP_PGS_CONTACT_FIRST, e.AGREE_SHIP_PGS_CONTACT_ID, e.AGREE_SHIP_PGS_CONTACT_LAST, e.AGREE_SHIP_PGS_CONTACT_PHONE, e.ASSET_OWNER_ACCNT_NAME, e.ASSET_OWNER_ID, e.ASSET_OWNER_ADDRESS_1, e.ASSET_OWNER_ADDRESS_2, e.ASSET_OWNER_ADDRESS_ID, e.ASSET_OWNER_CITY, e.ASSET_OWNER_COUNTRY, e.ASSET_OWNER_POSTAL, e.ASSET_OWNER_STATE, e.ASSET_CONTACT_EMAIL, e.ASSET_CONTACT_FIRST, e.ASSET_CONTACT_ID, e.ASSET_CONTACT_LAST, e.ASSET_CONTACT_PHONE, e.ASSET_STATUS, e.POLYCOM_TERRITORY, e.PORTAL_PRIMARY_AGREE_NAME, e.PORTAL_PRIMARY_AGREE_ID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY e
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON e.TMP_ID = edat.TMP_ID
	AND edat.MATCH_STATUS = 'Matched'
WHERE
	e.SO_DATE is NULL
	AND e.ENTL_CREATE_DATE is NULL

INTO OUTFILE '${EX_DIR}/polycom_entitlement_exception_matched_but_no_resolution_date_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	5.4 Inaccurate Transaction Amount exception report
*/
SELECT
	'EXCEPTION', 'AGREE_ID', 'AGREE_NAME', 'AGREE_NUM', 'AGREE_PO', 'AGREE_SALES_REGION', 'THEATER', 'AGREE_SO', 'NO_OF_ASSET_ENTL_ON_LINE', 'AGREE_LINE_ID', 'AGREE_LINE_NUM', 'AGREE_LINE_PART_LIST', 'AGREE_LINE_PART_NET', 'AGREE_LINE_PART_NET_PER_ASSET', 'ANNUALIZED_VALUE', 'AGREE_LINE_QTY', 'AGREE_LINE_CURRENCY', 'MSRP', 'MKTG_NAME', 'DISCOUNT_CODE', 'BUNDLE_PART_TRNS', 'AGREE_LINE_SERVICE_PART_NUM', 'PIM_DESCRIPTION', 'ASSET_NUM', 'ASSET_ID', 'ASSET_PO_NUM', 'ASSET_SERIAL_NUM', 'ASSET_SERVICE_REGION', 'ASSET_SHIP_DATE', 'ASSET_SHIP_YEAR', 'ASSET_SO_NUM', 'ASSET_ADDRESS_1', 'ASSET_ADDRESS_2', 'ASSET_ADDRESS_ID', 'ASSET_CITY', 'ASSET_COUNTRY', 'ASSET_POSTAL', 'ASSET_STATE', 'ASSET_PART_NUM', 'ASSET_PRODUCT', 'ASSET_PRODUCT_DIVISION', 'ASSET_PRODUCT_GROUP', 'ASSET_MASTER_PRODUCT_GROUP', 'EOSL', 'ASSET_PRODUCT_ID', 'ASSET_PRODUCT_LINE', 'ENTL_CREATE_DATE', 'ENTL_DELIVERY_TYPE', 'ENTL_END_DATE', 'ENTL_BATCH_QUARTER', 'ENTL_ID', 'ENTL_NAME', 'ENTL_NET_PRICE_PER_ASSET', 'ENTL_NET_PRICE_PER_ASSET_USD', 'ENTL_SERVICE_TYPE', 'ENTL_ST_DATE', 'PREV_AGREE_ID', 'PREV_ENTL_END', 'PREV_ENTL_PO', 'PREV_ENTL_SRVC_PART_NUM', 'PREV_ENTITLEMENT_ID', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE', 'SO_EXT_NET_PRICE_USD', 'AGREE_ACCNT_ID', 'AGREE_ACCNT_NAME', 'AGREE_MASTER_ACCNT_ID', 'AGREE_MSTR_ACCNT_NAME', 'AGREE_ACCNT_GAN', 'AGREE_ACCNT_ADDRESS_1', 'AGREE_ACCNT_ADDRESS_2', 'AGREE_ACCNT_ADDRESS_ID', 'AGREE_ACCNT_CITY', 'AGREE_ACCNT_COUNTRY', 'AGREE_ACCNT_POSTAL', 'AGREE_ACCNT_STATE', 'AGREE_ACCNT_CONTACT_EMAIL', 'AGREE_ACCNT_CONTACT_NAME', 'AGREE_ACCNT_CONTACT_PHONE', 'AGREE_ACCNT_PGS_CONTACT_EMAIL', 'AGREE_ACCNT_PGS_CONTACT_FIRST', 'AGREE_ACCNT_PGS_CONTACT_ID', 'AGREE_ACCNT_PGS_CONTACT_LAST', 'AGREE_ACCNT_PGS_CONTACT_PHONE', 'AGREE_END_CUST_ID', 'AGREE_END_CUST_MSTR_ID', 'AGREE_END_CUST_NAME', 'AGREE_END_CUST_MSTR_NAME', 'AGREE_END_CUST_ADDRESS_1', 'AGREE_END_CUST_ADDRESS_2', 'AGREE_END_CUST_ADDRESS_ID', 'AGREE_END_CUST_CITY', 'AGREE_END_CUST_COUNTRY', 'AGREE_END_CUST_POSTAL', 'AGREE_END_CUST_STATE', 'AGREE_END_CUST_CONTACT_EMAIL', 'AGREE_END_CUST_CONTACT_NAME', 'AGREE_END_CUST_CONTACT_PHONE', 'AGREE_END_CUST_PGS_CONTACT_EMAIL', 'AGREE_END_CUST_PGS_CONTACT_FIRST', 'AGREE_END_CUST_PGS_CONTACT_ID', 'AGREE_END_CUST_PGS_CONTACT_LAST', 'AGREE_END_CUST_PGS_CONTACT_PHONE', 'AGREE_RES_MASTER_ACCNT_ID', 'AGREE_RES_ACCNT_NAME', 'AGREE_RES_ACCT_MSTR_NAME', 'AGREE_RES_ID', 'AGREE_RES_ADDRESS_1', 'AGREE_RES_ADDRESS_2', 'AGREE_RES_ADDRESS_ID', 'AGREE_RES_CITY', 'AGREE_RES_COUNTRY', 'AGREE_RES_POSTAL', 'AGREE_RES_STATE', 'AGREE_RES_CONTACT_EMAIL', 'AGREE_RES_CONTAC_NAME', 'AGREE_RES_CONTACT_PHONE', 'AGREE_RES_PGS_CONTACT_EMAIL', 'AGREE_RES_PGS_CONTACT_FIRST', 'AGREE_RES_PGS_CONTACT_ID', 'AGREE_RES_PGS_CONTACT_LAST', 'AGREE_RES_PGS_CONTACT_PHONE', 'AGREE_SHIP_ACCNT_NAME', 'AGREE_SHIP_ID', 'AGREE_SHIP_ADDRESS_1', 'AGREE_SHIP_ADDRESS_2', 'AGREE_SHIP_ADDRESS_ID', 'AGREE_SHIP_CITY', 'AGREE_SHIP_COUNTRY', 'AGREE_SHIP_POSTAL', 'AGREE_SHIP_STATE', 'AGREE_SHIP_CONTACT_EMAIL', 'AGREE_SHIP_CONTACT_NAME', 'AGREE_SHIP_CONTACT_PHONE', 'AGREE_SHIP_PGS_CONTACT_EMAIL', 'AGREE_SHIP_PGS_CONTACT_FIRST', 'AGREE_SHIP_PGS_CONTACT_ID', 'AGREE_SHIP_PGS_CONTACT_LAST', 'AGREE_SHIP_PGS_CONTACT_PHONE', 'ASSET_OWNER_ACCNT_NAME', 'ASSET_OWNER_ID', 'ASSET_OWNER_ADDRESS_1', 'ASSET_OWNER_ADDRESS_2', 'ASSET_OWNER_ADDRESS_ID', 'ASSET_OWNER_CITY', 'ASSET_OWNER_COUNTRY', 'ASSET_OWNER_POSTAL', 'ASSET_OWNER_STATE', 'ASSET_CONTACT_EMAIL', 'ASSET_CONTACT_FIRST', 'ASSET_CONTACT_ID', 'ASSET_CONTACT_LAST', 'ASSET_CONTACT_PHONE', 'ASSET_STATUS', 'POLYCOM_TERRITORY', 'PORTAL_PRIMARY_AGREE_NAME', 'PORTAL_PRIMARY_AGREE_ID'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	'Inaccurate Transaction Amount',
	e.AGREE_ID, e.AGREE_NAME, e.AGREE_NUM, e.AGREE_PO, e.AGREE_SALES_REGION, e.THEATER, e.AGREE_SO, e.NO_OF_ASSET_ENTL_ON_LINE, e.AGREE_LINE_ID, e.AGREE_LINE_NUM, e.AGREE_LINE_PART_LIST, e.AGREE_LINE_PART_NET, e.AGREE_LINE_PART_NET_PER_ASSET, e.ANNUALIZED_VALUE, e.AGREE_LINE_QTY, e.AGREE_LINE_CURRENCY, e.MSRP, e.MKTG_NAME, e.DISCOUNT_CODE, e.BUNDLE_PART_TRNS, e.AGREE_LINE_SERVICE_PART_NUM, e.PIM_DESCRIPTION, e.ASSET_NUM, e.ASSET_ID, e.ASSET_PO_NUM, e.ASSET_SERIAL_NUM, e.ASSET_SERVICE_REGION, e.ASSET_SHIP_DATE, e.ASSET_SHIP_YEAR, e.ASSET_SO_NUM, e.ASSET_ADDRESS_1, e.ASSET_ADDRESS_2, e.ASSET_ADDRESS_ID, e.ASSET_CITY, e.ASSET_COUNTRY, e.ASSET_POSTAL, e.ASSET_STATE, e.ASSET_PART_NUM, e.ASSET_PRODUCT, e.ASSET_PRODUCT_DIVISION, e.ASSET_PRODUCT_GROUP, e.ASSET_MASTER_PRODUCT_GROUP, e.EOSL, e.ASSET_PRODUCT_ID, e.ASSET_PRODUCT_LINE, e.ENTL_CREATE_DATE, e.ENTL_DELIVERY_TYPE, e.ENTL_END_DATE, e.ENTL_BATCH_QUARTER, e.ENTL_ID, e.ENTL_NAME, e.ENTL_NET_PRICE_PER_ASSET, e.ENTL_NET_PRICE_PER_ASSET_USD, e.ENTL_SERVICE_TYPE, e.ENTL_ST_DATE, e.PREV_AGREE_ID, e.PREV_ENTL_END, e.PREV_ENTL_PO, e.PREV_ENTL_SRVC_PART_NUM, e.PREV_ENTITLEMENT_ID, e.SO_CURRENCY, e.SO_DATE, e.SO_EXT_NET_PRICE, e.SO_EXT_NET_PRICE_USD, e.AGREE_ACCNT_ID, e.AGREE_ACCNT_NAME, e.AGREE_MASTER_ACCNT_ID, e.AGREE_MSTR_ACCNT_NAME, e.AGREE_ACCNT_GAN, e.AGREE_ACCNT_ADDRESS_1, e.AGREE_ACCNT_ADDRESS_2, e.AGREE_ACCNT_ADDRESS_ID, e.AGREE_ACCNT_CITY, e.AGREE_ACCNT_COUNTRY, e.AGREE_ACCNT_POSTAL, e.AGREE_ACCNT_STATE, e.AGREE_ACCNT_CONTACT_EMAIL, e.AGREE_ACCNT_CONTACT_NAME, e.AGREE_ACCNT_CONTACT_PHONE, e.AGREE_ACCNT_PGS_CONTACT_EMAIL, e.AGREE_ACCNT_PGS_CONTACT_FIRST, e.AGREE_ACCNT_PGS_CONTACT_ID, e.AGREE_ACCNT_PGS_CONTACT_LAST, e.AGREE_ACCNT_PGS_CONTACT_PHONE, e.AGREE_END_CUST_ID, e.AGREE_END_CUST_MSTR_ID, e.AGREE_END_CUST_NAME, e.AGREE_END_CUST_MSTR_NAME, e.AGREE_END_CUST_ADDRESS_1, e.AGREE_END_CUST_ADDRESS_2, e.AGREE_END_CUST_ADDRESS_ID, e.AGREE_END_CUST_CITY, e.AGREE_END_CUST_COUNTRY, e.AGREE_END_CUST_POSTAL, e.AGREE_END_CUST_STATE, e.AGREE_END_CUST_CONTACT_EMAIL, e.AGREE_END_CUST_CONTACT_NAME, e.AGREE_END_CUST_CONTACT_PHONE, e.AGREE_END_CUST_PGS_CONTACT_EMAIL, e.AGREE_END_CUST_PGS_CONTACT_FIRST, e.AGREE_END_CUST_PGS_CONTACT_ID, e.AGREE_END_CUST_PGS_CONTACT_LAST, e.AGREE_END_CUST_PGS_CONTACT_PHONE, e.AGREE_RES_MASTER_ACCNT_ID, e.AGREE_RES_ACCNT_NAME, e.AGREE_RES_ACCT_MSTR_NAME, e.AGREE_RES_ID, e.AGREE_RES_ADDRESS_1, e.AGREE_RES_ADDRESS_2, e.AGREE_RES_ADDRESS_ID, e.AGREE_RES_CITY, e.AGREE_RES_COUNTRY, e.AGREE_RES_POSTAL, e.AGREE_RES_STATE, e.AGREE_RES_CONTACT_EMAIL, e.AGREE_RES_CONTAC_NAME, e.AGREE_RES_CONTACT_PHONE, e.AGREE_RES_PGS_CONTACT_EMAIL, e.AGREE_RES_PGS_CONTACT_FIRST, e.AGREE_RES_PGS_CONTACT_ID, e.AGREE_RES_PGS_CONTACT_LAST, e.AGREE_RES_PGS_CONTACT_PHONE, e.AGREE_SHIP_ACCNT_NAME, e.AGREE_SHIP_ID, e.AGREE_SHIP_ADDRESS_1, e.AGREE_SHIP_ADDRESS_2, e.AGREE_SHIP_ADDRESS_ID, e.AGREE_SHIP_CITY, e.AGREE_SHIP_COUNTRY, e.AGREE_SHIP_POSTAL, e.AGREE_SHIP_STATE, e.AGREE_SHIP_CONTACT_EMAIL, e.AGREE_SHIP_CONTACT_NAME, e.AGREE_SHIP_CONTACT_PHONE, e.AGREE_SHIP_PGS_CONTACT_EMAIL, e.AGREE_SHIP_PGS_CONTACT_FIRST, e.AGREE_SHIP_PGS_CONTACT_ID, e.AGREE_SHIP_PGS_CONTACT_LAST, e.AGREE_SHIP_PGS_CONTACT_PHONE, e.ASSET_OWNER_ACCNT_NAME, e.ASSET_OWNER_ID, e.ASSET_OWNER_ADDRESS_1, e.ASSET_OWNER_ADDRESS_2, e.ASSET_OWNER_ADDRESS_ID, e.ASSET_OWNER_CITY, e.ASSET_OWNER_COUNTRY, e.ASSET_OWNER_POSTAL, e.ASSET_OWNER_STATE, e.ASSET_CONTACT_EMAIL, e.ASSET_CONTACT_FIRST, e.ASSET_CONTACT_ID, e.ASSET_CONTACT_LAST, e.ASSET_CONTACT_PHONE, e.ASSET_STATUS, e.POLYCOM_TERRITORY, e.PORTAL_PRIMARY_AGREE_NAME, e.PORTAL_PRIMARY_AGREE_ID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY e
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.TMP_ID = e.TMP_ID
WHERE
	edat.MATCH_STATUS = 'Matched'
	AND e.AGREE_LINE_PART_NET_PER_ASSET > 0.0
	AND e.ENTL_NET_PRICE_PER_ASSET > 0.0 /* Avoid divide by zero */
	AND ABS(e.ENTL_NET_PRICE_PER_ASSET - e.AGREE_LINE_PART_NET_PER_ASSET)/e.ENTL_NET_PRICE_PER_ASSET > 0.1

INTO OUTFILE '${EX_DIR}/polycom_entitlement_exception_inaccurate_trans_amount_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	5.5 Zero Transaction Amount exception report
*/
SELECT
	'EXCEPTION', 'AGREE_ID', 'AGREE_NAME', 'AGREE_NUM', 'AGREE_PO', 'AGREE_SALES_REGION', 'THEATER', 'AGREE_SO', 'NO_OF_ASSET_ENTL_ON_LINE', 'AGREE_LINE_ID', 'AGREE_LINE_NUM', 'AGREE_LINE_PART_LIST', 'AGREE_LINE_PART_NET', 'AGREE_LINE_PART_NET_PER_ASSET', 'ANNUALIZED_VALUE', 'AGREE_LINE_QTY', 'AGREE_LINE_CURRENCY', 'MSRP', 'MKTG_NAME', 'DISCOUNT_CODE', 'BUNDLE_PART_TRNS', 'AGREE_LINE_SERVICE_PART_NUM', 'PIM_DESCRIPTION', 'ASSET_NUM', 'ASSET_ID', 'ASSET_PO_NUM', 'ASSET_SERIAL_NUM', 'ASSET_SERVICE_REGION', 'ASSET_SHIP_DATE', 'ASSET_SHIP_YEAR', 'ASSET_SO_NUM', 'ASSET_ADDRESS_1', 'ASSET_ADDRESS_2', 'ASSET_ADDRESS_ID', 'ASSET_CITY', 'ASSET_COUNTRY', 'ASSET_POSTAL', 'ASSET_STATE', 'ASSET_PART_NUM', 'ASSET_PRODUCT', 'ASSET_PRODUCT_DIVISION', 'ASSET_PRODUCT_GROUP', 'ASSET_MASTER_PRODUCT_GROUP', 'EOSL', 'ASSET_PRODUCT_ID', 'ASSET_PRODUCT_LINE', 'ENTL_CREATE_DATE', 'ENTL_DELIVERY_TYPE', 'ENTL_END_DATE', 'ENTL_BATCH_QUARTER', 'ENTL_ID', 'ENTL_NAME', 'ENTL_NET_PRICE_PER_ASSET', 'ENTL_NET_PRICE_PER_ASSET_USD', 'ENTL_SERVICE_TYPE', 'ENTL_ST_DATE', 'PREV_AGREE_ID', 'PREV_ENTL_END', 'PREV_ENTL_PO', 'PREV_ENTL_SRVC_PART_NUM', 'PREV_ENTITLEMENT_ID', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE', 'SO_EXT_NET_PRICE_USD', 'AGREE_ACCNT_ID', 'AGREE_ACCNT_NAME', 'AGREE_MASTER_ACCNT_ID', 'AGREE_MSTR_ACCNT_NAME', 'AGREE_ACCNT_GAN', 'AGREE_ACCNT_ADDRESS_1', 'AGREE_ACCNT_ADDRESS_2', 'AGREE_ACCNT_ADDRESS_ID', 'AGREE_ACCNT_CITY', 'AGREE_ACCNT_COUNTRY', 'AGREE_ACCNT_POSTAL', 'AGREE_ACCNT_STATE', 'AGREE_ACCNT_CONTACT_EMAIL', 'AGREE_ACCNT_CONTACT_NAME', 'AGREE_ACCNT_CONTACT_PHONE', 'AGREE_ACCNT_PGS_CONTACT_EMAIL', 'AGREE_ACCNT_PGS_CONTACT_FIRST', 'AGREE_ACCNT_PGS_CONTACT_ID', 'AGREE_ACCNT_PGS_CONTACT_LAST', 'AGREE_ACCNT_PGS_CONTACT_PHONE', 'AGREE_END_CUST_ID', 'AGREE_END_CUST_MSTR_ID', 'AGREE_END_CUST_NAME', 'AGREE_END_CUST_MSTR_NAME', 'AGREE_END_CUST_ADDRESS_1', 'AGREE_END_CUST_ADDRESS_2', 'AGREE_END_CUST_ADDRESS_ID', 'AGREE_END_CUST_CITY', 'AGREE_END_CUST_COUNTRY', 'AGREE_END_CUST_POSTAL', 'AGREE_END_CUST_STATE', 'AGREE_END_CUST_CONTACT_EMAIL', 'AGREE_END_CUST_CONTACT_NAME', 'AGREE_END_CUST_CONTACT_PHONE', 'AGREE_END_CUST_PGS_CONTACT_EMAIL', 'AGREE_END_CUST_PGS_CONTACT_FIRST', 'AGREE_END_CUST_PGS_CONTACT_ID', 'AGREE_END_CUST_PGS_CONTACT_LAST', 'AGREE_END_CUST_PGS_CONTACT_PHONE', 'AGREE_RES_MASTER_ACCNT_ID', 'AGREE_RES_ACCNT_NAME', 'AGREE_RES_ACCT_MSTR_NAME', 'AGREE_RES_ID', 'AGREE_RES_ADDRESS_1', 'AGREE_RES_ADDRESS_2', 'AGREE_RES_ADDRESS_ID', 'AGREE_RES_CITY', 'AGREE_RES_COUNTRY', 'AGREE_RES_POSTAL', 'AGREE_RES_STATE', 'AGREE_RES_CONTACT_EMAIL', 'AGREE_RES_CONTAC_NAME', 'AGREE_RES_CONTACT_PHONE', 'AGREE_RES_PGS_CONTACT_EMAIL', 'AGREE_RES_PGS_CONTACT_FIRST', 'AGREE_RES_PGS_CONTACT_ID', 'AGREE_RES_PGS_CONTACT_LAST', 'AGREE_RES_PGS_CONTACT_PHONE', 'AGREE_SHIP_ACCNT_NAME', 'AGREE_SHIP_ID', 'AGREE_SHIP_ADDRESS_1', 'AGREE_SHIP_ADDRESS_2', 'AGREE_SHIP_ADDRESS_ID', 'AGREE_SHIP_CITY', 'AGREE_SHIP_COUNTRY', 'AGREE_SHIP_POSTAL', 'AGREE_SHIP_STATE', 'AGREE_SHIP_CONTACT_EMAIL', 'AGREE_SHIP_CONTACT_NAME', 'AGREE_SHIP_CONTACT_PHONE', 'AGREE_SHIP_PGS_CONTACT_EMAIL', 'AGREE_SHIP_PGS_CONTACT_FIRST', 'AGREE_SHIP_PGS_CONTACT_ID', 'AGREE_SHIP_PGS_CONTACT_LAST', 'AGREE_SHIP_PGS_CONTACT_PHONE', 'ASSET_OWNER_ACCNT_NAME', 'ASSET_OWNER_ID', 'ASSET_OWNER_ADDRESS_1', 'ASSET_OWNER_ADDRESS_2', 'ASSET_OWNER_ADDRESS_ID', 'ASSET_OWNER_CITY', 'ASSET_OWNER_COUNTRY', 'ASSET_OWNER_POSTAL', 'ASSET_OWNER_STATE', 'ASSET_CONTACT_EMAIL', 'ASSET_CONTACT_FIRST', 'ASSET_CONTACT_ID', 'ASSET_CONTACT_LAST', 'ASSET_CONTACT_PHONE', 'ASSET_STATUS', 'POLYCOM_TERRITORY', 'PORTAL_PRIMARY_AGREE_NAME', 'PORTAL_PRIMARY_AGREE_ID'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	'Zero Transaction Amount',
	e.AGREE_ID, e.AGREE_NAME, e.AGREE_NUM, e.AGREE_PO, e.AGREE_SALES_REGION, e.THEATER, e.AGREE_SO, e.NO_OF_ASSET_ENTL_ON_LINE, e.AGREE_LINE_ID, e.AGREE_LINE_NUM, e.AGREE_LINE_PART_LIST, e.AGREE_LINE_PART_NET, e.AGREE_LINE_PART_NET_PER_ASSET, e.ANNUALIZED_VALUE, e.AGREE_LINE_QTY, e.AGREE_LINE_CURRENCY, e.MSRP, e.MKTG_NAME, e.DISCOUNT_CODE, e.BUNDLE_PART_TRNS, e.AGREE_LINE_SERVICE_PART_NUM, e.PIM_DESCRIPTION, e.ASSET_NUM, e.ASSET_ID, e.ASSET_PO_NUM, e.ASSET_SERIAL_NUM, e.ASSET_SERVICE_REGION, e.ASSET_SHIP_DATE, e.ASSET_SHIP_YEAR, e.ASSET_SO_NUM, e.ASSET_ADDRESS_1, e.ASSET_ADDRESS_2, e.ASSET_ADDRESS_ID, e.ASSET_CITY, e.ASSET_COUNTRY, e.ASSET_POSTAL, e.ASSET_STATE, e.ASSET_PART_NUM, e.ASSET_PRODUCT, e.ASSET_PRODUCT_DIVISION, e.ASSET_PRODUCT_GROUP, e.ASSET_MASTER_PRODUCT_GROUP, e.EOSL, e.ASSET_PRODUCT_ID, e.ASSET_PRODUCT_LINE, e.ENTL_CREATE_DATE, e.ENTL_DELIVERY_TYPE, e.ENTL_END_DATE, e.ENTL_BATCH_QUARTER, e.ENTL_ID, e.ENTL_NAME, e.ENTL_NET_PRICE_PER_ASSET, e.ENTL_NET_PRICE_PER_ASSET_USD, e.ENTL_SERVICE_TYPE, e.ENTL_ST_DATE, e.PREV_AGREE_ID, e.PREV_ENTL_END, e.PREV_ENTL_PO, e.PREV_ENTL_SRVC_PART_NUM, e.PREV_ENTITLEMENT_ID, e.SO_CURRENCY, e.SO_DATE, e.SO_EXT_NET_PRICE, e.SO_EXT_NET_PRICE_USD, e.AGREE_ACCNT_ID, e.AGREE_ACCNT_NAME, e.AGREE_MASTER_ACCNT_ID, e.AGREE_MSTR_ACCNT_NAME, e.AGREE_ACCNT_GAN, e.AGREE_ACCNT_ADDRESS_1, e.AGREE_ACCNT_ADDRESS_2, e.AGREE_ACCNT_ADDRESS_ID, e.AGREE_ACCNT_CITY, e.AGREE_ACCNT_COUNTRY, e.AGREE_ACCNT_POSTAL, e.AGREE_ACCNT_STATE, e.AGREE_ACCNT_CONTACT_EMAIL, e.AGREE_ACCNT_CONTACT_NAME, e.AGREE_ACCNT_CONTACT_PHONE, e.AGREE_ACCNT_PGS_CONTACT_EMAIL, e.AGREE_ACCNT_PGS_CONTACT_FIRST, e.AGREE_ACCNT_PGS_CONTACT_ID, e.AGREE_ACCNT_PGS_CONTACT_LAST, e.AGREE_ACCNT_PGS_CONTACT_PHONE, e.AGREE_END_CUST_ID, e.AGREE_END_CUST_MSTR_ID, e.AGREE_END_CUST_NAME, e.AGREE_END_CUST_MSTR_NAME, e.AGREE_END_CUST_ADDRESS_1, e.AGREE_END_CUST_ADDRESS_2, e.AGREE_END_CUST_ADDRESS_ID, e.AGREE_END_CUST_CITY, e.AGREE_END_CUST_COUNTRY, e.AGREE_END_CUST_POSTAL, e.AGREE_END_CUST_STATE, e.AGREE_END_CUST_CONTACT_EMAIL, e.AGREE_END_CUST_CONTACT_NAME, e.AGREE_END_CUST_CONTACT_PHONE, e.AGREE_END_CUST_PGS_CONTACT_EMAIL, e.AGREE_END_CUST_PGS_CONTACT_FIRST, e.AGREE_END_CUST_PGS_CONTACT_ID, e.AGREE_END_CUST_PGS_CONTACT_LAST, e.AGREE_END_CUST_PGS_CONTACT_PHONE, e.AGREE_RES_MASTER_ACCNT_ID, e.AGREE_RES_ACCNT_NAME, e.AGREE_RES_ACCT_MSTR_NAME, e.AGREE_RES_ID, e.AGREE_RES_ADDRESS_1, e.AGREE_RES_ADDRESS_2, e.AGREE_RES_ADDRESS_ID, e.AGREE_RES_CITY, e.AGREE_RES_COUNTRY, e.AGREE_RES_POSTAL, e.AGREE_RES_STATE, e.AGREE_RES_CONTACT_EMAIL, e.AGREE_RES_CONTAC_NAME, e.AGREE_RES_CONTACT_PHONE, e.AGREE_RES_PGS_CONTACT_EMAIL, e.AGREE_RES_PGS_CONTACT_FIRST, e.AGREE_RES_PGS_CONTACT_ID, e.AGREE_RES_PGS_CONTACT_LAST, e.AGREE_RES_PGS_CONTACT_PHONE, e.AGREE_SHIP_ACCNT_NAME, e.AGREE_SHIP_ID, e.AGREE_SHIP_ADDRESS_1, e.AGREE_SHIP_ADDRESS_2, e.AGREE_SHIP_ADDRESS_ID, e.AGREE_SHIP_CITY, e.AGREE_SHIP_COUNTRY, e.AGREE_SHIP_POSTAL, e.AGREE_SHIP_STATE, e.AGREE_SHIP_CONTACT_EMAIL, e.AGREE_SHIP_CONTACT_NAME, e.AGREE_SHIP_CONTACT_PHONE, e.AGREE_SHIP_PGS_CONTACT_EMAIL, e.AGREE_SHIP_PGS_CONTACT_FIRST, e.AGREE_SHIP_PGS_CONTACT_ID, e.AGREE_SHIP_PGS_CONTACT_LAST, e.AGREE_SHIP_PGS_CONTACT_PHONE, e.ASSET_OWNER_ACCNT_NAME, e.ASSET_OWNER_ID, e.ASSET_OWNER_ADDRESS_1, e.ASSET_OWNER_ADDRESS_2, e.ASSET_OWNER_ADDRESS_ID, e.ASSET_OWNER_CITY, e.ASSET_OWNER_COUNTRY, e.ASSET_OWNER_POSTAL, e.ASSET_OWNER_STATE, e.ASSET_CONTACT_EMAIL, e.ASSET_CONTACT_FIRST, e.ASSET_CONTACT_ID, e.ASSET_CONTACT_LAST, e.ASSET_CONTACT_PHONE, e.ASSET_STATUS, e.POLYCOM_TERRITORY, e.PORTAL_PRIMARY_AGREE_NAME, e.PORTAL_PRIMARY_AGREE_ID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY e
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON e.TMP_ID = edat.TMP_ID
	AND edat.MATCH_STATUS = 'Matched'
WHERE
	COALESCE(e.AGREE_LINE_PART_NET_PER_ASSET, 0) = 0

INTO OUTFILE '${EX_DIR}/polycom_entitlement_exception_zero_trans_amount_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	5.6 Blank Booking Date exception report
*/
SELECT
	'EXCEPTION', 'AGREE_ID', 'AGREE_NAME', 'AGREE_NUM', 'AGREE_PO', 'AGREE_SALES_REGION', 'THEATER', 'AGREE_SO', 'NO_OF_ASSET_ENTL_ON_LINE', 'AGREE_LINE_ID', 'AGREE_LINE_NUM', 'AGREE_LINE_PART_LIST', 'AGREE_LINE_PART_NET', 'AGREE_LINE_PART_NET_PER_ASSET', 'ANNUALIZED_VALUE', 'AGREE_LINE_QTY', 'AGREE_LINE_CURRENCY', 'MSRP', 'MKTG_NAME', 'DISCOUNT_CODE', 'BUNDLE_PART_TRNS', 'AGREE_LINE_SERVICE_PART_NUM', 'PIM_DESCRIPTION', 'ASSET_NUM', 'ASSET_ID', 'ASSET_PO_NUM', 'ASSET_SERIAL_NUM', 'ASSET_SERVICE_REGION', 'ASSET_SHIP_DATE', 'ASSET_SHIP_YEAR', 'ASSET_SO_NUM', 'ASSET_ADDRESS_1', 'ASSET_ADDRESS_2', 'ASSET_ADDRESS_ID', 'ASSET_CITY', 'ASSET_COUNTRY', 'ASSET_POSTAL', 'ASSET_STATE', 'ASSET_PART_NUM', 'ASSET_PRODUCT', 'ASSET_PRODUCT_DIVISION', 'ASSET_PRODUCT_GROUP', 'ASSET_MASTER_PRODUCT_GROUP', 'EOSL', 'ASSET_PRODUCT_ID', 'ASSET_PRODUCT_LINE', 'ENTL_CREATE_DATE', 'ENTL_DELIVERY_TYPE', 'ENTL_END_DATE', 'ENTL_BATCH_QUARTER', 'ENTL_ID', 'ENTL_NAME', 'ENTL_NET_PRICE_PER_ASSET', 'ENTL_NET_PRICE_PER_ASSET_USD', 'ENTL_SERVICE_TYPE', 'ENTL_ST_DATE', 'PREV_AGREE_ID', 'PREV_ENTL_END', 'PREV_ENTL_PO', 'PREV_ENTL_SRVC_PART_NUM', 'PREV_ENTITLEMENT_ID', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE', 'SO_EXT_NET_PRICE_USD', 'AGREE_ACCNT_ID', 'AGREE_ACCNT_NAME', 'AGREE_MASTER_ACCNT_ID', 'AGREE_MSTR_ACCNT_NAME', 'AGREE_ACCNT_GAN', 'AGREE_ACCNT_ADDRESS_1', 'AGREE_ACCNT_ADDRESS_2', 'AGREE_ACCNT_ADDRESS_ID', 'AGREE_ACCNT_CITY', 'AGREE_ACCNT_COUNTRY', 'AGREE_ACCNT_POSTAL', 'AGREE_ACCNT_STATE', 'AGREE_ACCNT_CONTACT_EMAIL', 'AGREE_ACCNT_CONTACT_NAME', 'AGREE_ACCNT_CONTACT_PHONE', 'AGREE_ACCNT_PGS_CONTACT_EMAIL', 'AGREE_ACCNT_PGS_CONTACT_FIRST', 'AGREE_ACCNT_PGS_CONTACT_ID', 'AGREE_ACCNT_PGS_CONTACT_LAST', 'AGREE_ACCNT_PGS_CONTACT_PHONE', 'AGREE_END_CUST_ID', 'AGREE_END_CUST_MSTR_ID', 'AGREE_END_CUST_NAME', 'AGREE_END_CUST_MSTR_NAME', 'AGREE_END_CUST_ADDRESS_1', 'AGREE_END_CUST_ADDRESS_2', 'AGREE_END_CUST_ADDRESS_ID', 'AGREE_END_CUST_CITY', 'AGREE_END_CUST_COUNTRY', 'AGREE_END_CUST_POSTAL', 'AGREE_END_CUST_STATE', 'AGREE_END_CUST_CONTACT_EMAIL', 'AGREE_END_CUST_CONTACT_NAME', 'AGREE_END_CUST_CONTACT_PHONE', 'AGREE_END_CUST_PGS_CONTACT_EMAIL', 'AGREE_END_CUST_PGS_CONTACT_FIRST', 'AGREE_END_CUST_PGS_CONTACT_ID', 'AGREE_END_CUST_PGS_CONTACT_LAST', 'AGREE_END_CUST_PGS_CONTACT_PHONE', 'AGREE_RES_MASTER_ACCNT_ID', 'AGREE_RES_ACCNT_NAME', 'AGREE_RES_ACCT_MSTR_NAME', 'AGREE_RES_ID', 'AGREE_RES_ADDRESS_1', 'AGREE_RES_ADDRESS_2', 'AGREE_RES_ADDRESS_ID', 'AGREE_RES_CITY', 'AGREE_RES_COUNTRY', 'AGREE_RES_POSTAL', 'AGREE_RES_STATE', 'AGREE_RES_CONTACT_EMAIL', 'AGREE_RES_CONTAC_NAME', 'AGREE_RES_CONTACT_PHONE', 'AGREE_RES_PGS_CONTACT_EMAIL', 'AGREE_RES_PGS_CONTACT_FIRST', 'AGREE_RES_PGS_CONTACT_ID', 'AGREE_RES_PGS_CONTACT_LAST', 'AGREE_RES_PGS_CONTACT_PHONE', 'AGREE_SHIP_ACCNT_NAME', 'AGREE_SHIP_ID', 'AGREE_SHIP_ADDRESS_1', 'AGREE_SHIP_ADDRESS_2', 'AGREE_SHIP_ADDRESS_ID', 'AGREE_SHIP_CITY', 'AGREE_SHIP_COUNTRY', 'AGREE_SHIP_POSTAL', 'AGREE_SHIP_STATE', 'AGREE_SHIP_CONTACT_EMAIL', 'AGREE_SHIP_CONTACT_NAME', 'AGREE_SHIP_CONTACT_PHONE', 'AGREE_SHIP_PGS_CONTACT_EMAIL', 'AGREE_SHIP_PGS_CONTACT_FIRST', 'AGREE_SHIP_PGS_CONTACT_ID', 'AGREE_SHIP_PGS_CONTACT_LAST', 'AGREE_SHIP_PGS_CONTACT_PHONE', 'ASSET_OWNER_ACCNT_NAME', 'ASSET_OWNER_ID', 'ASSET_OWNER_ADDRESS_1', 'ASSET_OWNER_ADDRESS_2', 'ASSET_OWNER_ADDRESS_ID', 'ASSET_OWNER_CITY', 'ASSET_OWNER_COUNTRY', 'ASSET_OWNER_POSTAL', 'ASSET_OWNER_STATE', 'ASSET_CONTACT_EMAIL', 'ASSET_CONTACT_FIRST', 'ASSET_CONTACT_ID', 'ASSET_CONTACT_LAST', 'ASSET_CONTACT_PHONE', 'ASSET_STATUS', 'POLYCOM_TERRITORY', 'PORTAL_PRIMARY_AGREE_NAME', 'PORTAL_PRIMARY_AGREE_ID'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	'Blank Booking Date',
	e.AGREE_ID, e.AGREE_NAME, e.AGREE_NUM, e.AGREE_PO, e.AGREE_SALES_REGION, e.THEATER, e.AGREE_SO, e.NO_OF_ASSET_ENTL_ON_LINE, e.AGREE_LINE_ID, e.AGREE_LINE_NUM, e.AGREE_LINE_PART_LIST, e.AGREE_LINE_PART_NET, e.AGREE_LINE_PART_NET_PER_ASSET, e.ANNUALIZED_VALUE, e.AGREE_LINE_QTY, e.AGREE_LINE_CURRENCY, e.MSRP, e.MKTG_NAME, e.DISCOUNT_CODE, e.BUNDLE_PART_TRNS, e.AGREE_LINE_SERVICE_PART_NUM, e.PIM_DESCRIPTION, e.ASSET_NUM, e.ASSET_ID, e.ASSET_PO_NUM, e.ASSET_SERIAL_NUM, e.ASSET_SERVICE_REGION, e.ASSET_SHIP_DATE, e.ASSET_SHIP_YEAR, e.ASSET_SO_NUM, e.ASSET_ADDRESS_1, e.ASSET_ADDRESS_2, e.ASSET_ADDRESS_ID, e.ASSET_CITY, e.ASSET_COUNTRY, e.ASSET_POSTAL, e.ASSET_STATE, e.ASSET_PART_NUM, e.ASSET_PRODUCT, e.ASSET_PRODUCT_DIVISION, e.ASSET_PRODUCT_GROUP, e.ASSET_MASTER_PRODUCT_GROUP, e.EOSL, e.ASSET_PRODUCT_ID, e.ASSET_PRODUCT_LINE, e.ENTL_CREATE_DATE, e.ENTL_DELIVERY_TYPE, e.ENTL_END_DATE, e.ENTL_BATCH_QUARTER, e.ENTL_ID, e.ENTL_NAME, e.ENTL_NET_PRICE_PER_ASSET, e.ENTL_NET_PRICE_PER_ASSET_USD, e.ENTL_SERVICE_TYPE, e.ENTL_ST_DATE, e.PREV_AGREE_ID, e.PREV_ENTL_END, e.PREV_ENTL_PO, e.PREV_ENTL_SRVC_PART_NUM, e.PREV_ENTITLEMENT_ID, e.SO_CURRENCY, e.SO_DATE, e.SO_EXT_NET_PRICE, e.SO_EXT_NET_PRICE_USD, e.AGREE_ACCNT_ID, e.AGREE_ACCNT_NAME, e.AGREE_MASTER_ACCNT_ID, e.AGREE_MSTR_ACCNT_NAME, e.AGREE_ACCNT_GAN, e.AGREE_ACCNT_ADDRESS_1, e.AGREE_ACCNT_ADDRESS_2, e.AGREE_ACCNT_ADDRESS_ID, e.AGREE_ACCNT_CITY, e.AGREE_ACCNT_COUNTRY, e.AGREE_ACCNT_POSTAL, e.AGREE_ACCNT_STATE, e.AGREE_ACCNT_CONTACT_EMAIL, e.AGREE_ACCNT_CONTACT_NAME, e.AGREE_ACCNT_CONTACT_PHONE, e.AGREE_ACCNT_PGS_CONTACT_EMAIL, e.AGREE_ACCNT_PGS_CONTACT_FIRST, e.AGREE_ACCNT_PGS_CONTACT_ID, e.AGREE_ACCNT_PGS_CONTACT_LAST, e.AGREE_ACCNT_PGS_CONTACT_PHONE, e.AGREE_END_CUST_ID, e.AGREE_END_CUST_MSTR_ID, e.AGREE_END_CUST_NAME, e.AGREE_END_CUST_MSTR_NAME, e.AGREE_END_CUST_ADDRESS_1, e.AGREE_END_CUST_ADDRESS_2, e.AGREE_END_CUST_ADDRESS_ID, e.AGREE_END_CUST_CITY, e.AGREE_END_CUST_COUNTRY, e.AGREE_END_CUST_POSTAL, e.AGREE_END_CUST_STATE, e.AGREE_END_CUST_CONTACT_EMAIL, e.AGREE_END_CUST_CONTACT_NAME, e.AGREE_END_CUST_CONTACT_PHONE, e.AGREE_END_CUST_PGS_CONTACT_EMAIL, e.AGREE_END_CUST_PGS_CONTACT_FIRST, e.AGREE_END_CUST_PGS_CONTACT_ID, e.AGREE_END_CUST_PGS_CONTACT_LAST, e.AGREE_END_CUST_PGS_CONTACT_PHONE, e.AGREE_RES_MASTER_ACCNT_ID, e.AGREE_RES_ACCNT_NAME, e.AGREE_RES_ACCT_MSTR_NAME, e.AGREE_RES_ID, e.AGREE_RES_ADDRESS_1, e.AGREE_RES_ADDRESS_2, e.AGREE_RES_ADDRESS_ID, e.AGREE_RES_CITY, e.AGREE_RES_COUNTRY, e.AGREE_RES_POSTAL, e.AGREE_RES_STATE, e.AGREE_RES_CONTACT_EMAIL, e.AGREE_RES_CONTAC_NAME, e.AGREE_RES_CONTACT_PHONE, e.AGREE_RES_PGS_CONTACT_EMAIL, e.AGREE_RES_PGS_CONTACT_FIRST, e.AGREE_RES_PGS_CONTACT_ID, e.AGREE_RES_PGS_CONTACT_LAST, e.AGREE_RES_PGS_CONTACT_PHONE, e.AGREE_SHIP_ACCNT_NAME, e.AGREE_SHIP_ID, e.AGREE_SHIP_ADDRESS_1, e.AGREE_SHIP_ADDRESS_2, e.AGREE_SHIP_ADDRESS_ID, e.AGREE_SHIP_CITY, e.AGREE_SHIP_COUNTRY, e.AGREE_SHIP_POSTAL, e.AGREE_SHIP_STATE, e.AGREE_SHIP_CONTACT_EMAIL, e.AGREE_SHIP_CONTACT_NAME, e.AGREE_SHIP_CONTACT_PHONE, e.AGREE_SHIP_PGS_CONTACT_EMAIL, e.AGREE_SHIP_PGS_CONTACT_FIRST, e.AGREE_SHIP_PGS_CONTACT_ID, e.AGREE_SHIP_PGS_CONTACT_LAST, e.AGREE_SHIP_PGS_CONTACT_PHONE, e.ASSET_OWNER_ACCNT_NAME, e.ASSET_OWNER_ID, e.ASSET_OWNER_ADDRESS_1, e.ASSET_OWNER_ADDRESS_2, e.ASSET_OWNER_ADDRESS_ID, e.ASSET_OWNER_CITY, e.ASSET_OWNER_COUNTRY, e.ASSET_OWNER_POSTAL, e.ASSET_OWNER_STATE, e.ASSET_CONTACT_EMAIL, e.ASSET_CONTACT_FIRST, e.ASSET_CONTACT_ID, e.ASSET_CONTACT_LAST, e.ASSET_CONTACT_PHONE, e.ASSET_STATUS, e.POLYCOM_TERRITORY, e.PORTAL_PRIMARY_AGREE_NAME, e.PORTAL_PRIMARY_AGREE_ID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY e
WHERE
	e.SO_DATE is NULL
	AND e.ENTL_CREATE_DATE is NULL
	${ENTITLEMENT_EXCEPTION_DATE_CLAUSE}

INTO OUTFILE '${EX_DIR}/polycom_entitlement_exception_blank_booking_date_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	5.6 FDM 'Closed Sale' exception report
*/
SELECT
	'EXCEPTION', 'AGREE_PO', 'AGREE_SO', 'AGREE_ACCNT_NAME', 'AGREE_LINE_SERVICE_PART_NUM', 'AGREE_LINE_SERVICE_PRODUCT', 'AGREE_LINE_PRODUCT_GROUP', 'AGREE_SHIP_ACCNT_NAME', 'INVOICE_DATE', 'INVOICE_NUM', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE_USD', 'SO_EXT_NET_PRICE', 'DOC_CURR_CODE', 'LOC_CURR_CODE'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	'Matched, but already Closed Sale',
	doa.AGREE_PO, doa.AGREE_SO, doa.AGREE_ACCNT_NAME, doa.AGREE_LINE_SERVICE_PART_NUM, doa.AGREE_LINE_SERVICE_PRODUCT, doa.AGREE_LINE_PRODUCT_GROUP, doa.AGREE_SHIP_ACCNT_NAME, doa.INVOICE_DATE, doa.INVOICE_NUM, doa.SO_CURRENCY, doa.SO_DATE, doa.SO_EXT_NET_PRICE_USD, doa.SO_EXT_NET_PRICE, doa.DOC_CURR_CODE, doa.LOC_CURR_CODE
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = t_offers.OPPID
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = t_offers.OFFID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.TMP_ID = edat.TMP_ID
INNER JOIN polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY doa
	ON doa.AGREE_PO = eda.AGREE_PO /* offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE */
WHERE
	doa.AGREE_LINE_PRODUCT_GROUP = '2015'
	AND doa.AGREE_PO IS NOT NULL
	AND doa.AGREE_PO NOT LIKE '%BUNDLE%'
	AND doa.AGREE_LINE_SERVICE_PRODUCT LIKE '%fee%'
	AND opp.FLOWS_SALESSTAGES_STATE_NAME = 'closedSale'

INTO OUTFILE '${EX_DIR}/polycom_daily_order_exception_closed_sale_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	5.7 FDM AGREE_PO Already Used exception report
**	No exception report
*/
SELECT
	'EXCEPTION', 'AGREE_PO', 'AGREE_SO', 'AGREE_ACCNT_NAME', 'AGREE_LINE_SERVICE_PART_NUM', 'AGREE_LINE_SERVICE_PRODUCT', 'AGREE_LINE_PRODUCT_GROUP', 'AGREE_SHIP_ACCNT_NAME', 'INVOICE_DATE', 'INVOICE_NUM', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE_USD', 'SO_EXT_NET_PRICE', 'DOC_CURR_CODE', 'LOC_CURR_CODE'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	'Re-activation previously calculated for this AGREE_PO', -- <TODO> Confirm this message
	doa.AGREE_PO, doa.AGREE_SO, doa.AGREE_ACCNT_NAME, doa.AGREE_LINE_SERVICE_PART_NUM, doa.AGREE_LINE_SERVICE_PRODUCT, doa.AGREE_LINE_PRODUCT_GROUP, doa.AGREE_SHIP_ACCNT_NAME, doa.INVOICE_DATE, doa.INVOICE_NUM, doa.SO_CURRENCY, doa.SO_DATE, doa.SO_EXT_NET_PRICE_USD, doa.SO_EXT_NET_PRICE, doa.DOC_CURR_CODE, doa.LOC_CURR_CODE
FROM
	polycom_tmp.opp_entitlement_tmp opp_e
INNER JOIN ${SCHEMA}.APP_OPPORTUNITIES opp
	ON opp._ID = opp_e.OPP_ID
INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
	ON t_offers.OPPID = opp._ID
INNER JOIN ${SCHEMA}.APP_OFFERS offers
	ON offers._ID = t_offers.OFFID
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON edat.OFFER_ID = offers._ID
	AND edat.MATCH_STATUS = 'Matched'
INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
	ON eda.TMP_ID = edat.TMP_ID
INNER JOIN polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY doa
	ON doa.AGREE_PO = eda.AGREE_PO /* offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE */
INNER JOIN polycom_data.REACTIVATED_AGREE_PO rap
	ON doa.AGREE_PO = rap.AGREE_PO
WHERE
	doa.AGREE_LINE_PRODUCT_GROUP = '2015'
	AND doa.AGREE_PO IS NOT NULL
	AND doa.AGREE_PO NOT LIKE '%BUNDLE%'
	AND doa.AGREE_LINE_SERVICE_PRODUCT LIKE '%fee%'
	AND opp.FLOWS_SALESSTAGES_STATE_NAME = 'closedSale'

INTO OUTFILE '${EX_DIR}/polycom_daily_order_exception_agree_po_already_reactivated_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	5.8 FDM 'Not Matched' exception report
*/
SELECT
	'EXCEPTION', 'AGREE_PO', 'AGREE_SO', 'AGREE_ACCNT_NAME', 'AGREE_LINE_SERVICE_PART_NUM', 'AGREE_LINE_SERVICE_PRODUCT', 'AGREE_LINE_PRODUCT_GROUP', 'AGREE_SHIP_ACCNT_NAME', 'INVOICE_DATE', 'INVOICE_NUM', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE_USD', 'SO_EXT_NET_PRICE', 'DOC_CURR_CODE', 'LOC_CURR_CODE'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	'Failed to Match',
	doa.AGREE_PO, doa.AGREE_SO, doa.AGREE_ACCNT_NAME, doa.AGREE_LINE_SERVICE_PART_NUM, doa.AGREE_LINE_SERVICE_PRODUCT, doa.AGREE_LINE_PRODUCT_GROUP, doa.AGREE_SHIP_ACCNT_NAME, doa.INVOICE_DATE, doa.INVOICE_NUM, doa.SO_CURRENCY, doa.SO_DATE, doa.SO_EXT_NET_PRICE_USD, doa.SO_EXT_NET_PRICE, doa.DOC_CURR_CODE, doa.LOC_CURR_CODE
FROM
	polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY doa
WHERE
	doa.TMP_ID NOT IN (
	SELECT
		doai.TMP_ID
	FROM
		polycom_tmp.opp_entitlement_tmp opp_e
	INNER JOIN ${SCHEMA}.T_ACTIVE_OFFERS t_offers
		ON t_offers.OPPID = opp_e.OPP_ID
	INNER JOIN ${SCHEMA}.APP_OFFERS offers
		ON offers._ID = t_offers.OFFID
	INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
		ON edat.OFFER_ID = t_offers.OFFID
		AND edat.MATCH_STATUS = 'Matched'
	INNER JOIN polycom_data.ENTITLEMENT_DAILY_ACTIVITY eda
		ON eda.TMP_ID = edat.TMP_ID
	INNER JOIN polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY doai
		ON doai.AGREE_PO = eda.AGREE_PO /* offers.EXTENSIONS_MASTER_EXISTINGPONUMBER_VALUE */
	WHERE
		doai.AGREE_LINE_PRODUCT_GROUP = '2015'
		AND doai.AGREE_PO IS NOT NULL
		AND doai.AGREE_PO NOT LIKE '%BUNDLE%'
		AND doai.AGREE_LINE_SERVICE_PRODUCT LIKE '%fee%'
	)
	${ORDER_EXCEPTION_DATE_CLAUSE}

INTO OUTFILE '${EX_DIR}/polycom_daily_order_exception_failed_to_match_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;
/*
**	5.9 Blank Booking Date exception report
*/
SELECT
	'EXCEPTION', 'AGREE_ID', 'AGREE_NAME', 'AGREE_NUM', 'AGREE_PO', 'AGREE_SALES_REGION', 'THEATER', 'AGREE_SO', 'NO_OF_ASSET_ENTL_ON_LINE', 'AGREE_LINE_ID', 'AGREE_LINE_NUM', 'AGREE_LINE_PART_LIST', 'AGREE_LINE_PART_NET', 'AGREE_LINE_PART_NET_PER_ASSET', 'ANNUALIZED_VALUE', 'AGREE_LINE_QTY', 'AGREE_LINE_CURRENCY', 'MSRP', 'MKTG_NAME', 'DISCOUNT_CODE', 'BUNDLE_PART_TRNS', 'AGREE_LINE_SERVICE_PART_NUM', 'PIM_DESCRIPTION', 'ASSET_NUM', 'ASSET_ID', 'ASSET_PO_NUM', 'ASSET_SERIAL_NUM', 'ASSET_SERVICE_REGION', 'ASSET_SHIP_DATE', 'ASSET_SHIP_YEAR', 'ASSET_SO_NUM', 'ASSET_ADDRESS_1', 'ASSET_ADDRESS_2', 'ASSET_ADDRESS_ID', 'ASSET_CITY', 'ASSET_COUNTRY', 'ASSET_POSTAL', 'ASSET_STATE', 'ASSET_PART_NUM', 'ASSET_PRODUCT', 'ASSET_PRODUCT_DIVISION', 'ASSET_PRODUCT_GROUP', 'ASSET_MASTER_PRODUCT_GROUP', 'EOSL', 'ASSET_PRODUCT_ID', 'ASSET_PRODUCT_LINE', 'ENTL_CREATE_DATE', 'ENTL_DELIVERY_TYPE', 'ENTL_END_DATE', 'ENTL_BATCH_QUARTER', 'ENTL_ID', 'ENTL_NAME', 'ENTL_NET_PRICE_PER_ASSET', 'ENTL_NET_PRICE_PER_ASSET_USD', 'ENTL_SERVICE_TYPE', 'ENTL_ST_DATE', 'PREV_AGREE_ID', 'PREV_ENTL_END', 'PREV_ENTL_PO', 'PREV_ENTL_SRVC_PART_NUM', 'PREV_ENTITLEMENT_ID', 'SO_CURRENCY', 'SO_DATE', 'SO_EXT_NET_PRICE', 'SO_EXT_NET_PRICE_USD', 'AGREE_ACCNT_ID', 'AGREE_ACCNT_NAME', 'AGREE_MASTER_ACCNT_ID', 'AGREE_MSTR_ACCNT_NAME', 'AGREE_ACCNT_GAN', 'AGREE_ACCNT_ADDRESS_1', 'AGREE_ACCNT_ADDRESS_2', 'AGREE_ACCNT_ADDRESS_ID', 'AGREE_ACCNT_CITY', 'AGREE_ACCNT_COUNTRY', 'AGREE_ACCNT_POSTAL', 'AGREE_ACCNT_STATE', 'AGREE_ACCNT_CONTACT_EMAIL', 'AGREE_ACCNT_CONTACT_NAME', 'AGREE_ACCNT_CONTACT_PHONE', 'AGREE_ACCNT_PGS_CONTACT_EMAIL', 'AGREE_ACCNT_PGS_CONTACT_FIRST', 'AGREE_ACCNT_PGS_CONTACT_ID', 'AGREE_ACCNT_PGS_CONTACT_LAST', 'AGREE_ACCNT_PGS_CONTACT_PHONE', 'AGREE_END_CUST_ID', 'AGREE_END_CUST_MSTR_ID', 'AGREE_END_CUST_NAME', 'AGREE_END_CUST_MSTR_NAME', 'AGREE_END_CUST_ADDRESS_1', 'AGREE_END_CUST_ADDRESS_2', 'AGREE_END_CUST_ADDRESS_ID', 'AGREE_END_CUST_CITY', 'AGREE_END_CUST_COUNTRY', 'AGREE_END_CUST_POSTAL', 'AGREE_END_CUST_STATE', 'AGREE_END_CUST_CONTACT_EMAIL', 'AGREE_END_CUST_CONTACT_NAME', 'AGREE_END_CUST_CONTACT_PHONE', 'AGREE_END_CUST_PGS_CONTACT_EMAIL', 'AGREE_END_CUST_PGS_CONTACT_FIRST', 'AGREE_END_CUST_PGS_CONTACT_ID', 'AGREE_END_CUST_PGS_CONTACT_LAST', 'AGREE_END_CUST_PGS_CONTACT_PHONE', 'AGREE_RES_MASTER_ACCNT_ID', 'AGREE_RES_ACCNT_NAME', 'AGREE_RES_ACCT_MSTR_NAME', 'AGREE_RES_ID', 'AGREE_RES_ADDRESS_1', 'AGREE_RES_ADDRESS_2', 'AGREE_RES_ADDRESS_ID', 'AGREE_RES_CITY', 'AGREE_RES_COUNTRY', 'AGREE_RES_POSTAL', 'AGREE_RES_STATE', 'AGREE_RES_CONTACT_EMAIL', 'AGREE_RES_CONTAC_NAME', 'AGREE_RES_CONTACT_PHONE', 'AGREE_RES_PGS_CONTACT_EMAIL', 'AGREE_RES_PGS_CONTACT_FIRST', 'AGREE_RES_PGS_CONTACT_ID', 'AGREE_RES_PGS_CONTACT_LAST', 'AGREE_RES_PGS_CONTACT_PHONE', 'AGREE_SHIP_ACCNT_NAME', 'AGREE_SHIP_ID', 'AGREE_SHIP_ADDRESS_1', 'AGREE_SHIP_ADDRESS_2', 'AGREE_SHIP_ADDRESS_ID', 'AGREE_SHIP_CITY', 'AGREE_SHIP_COUNTRY', 'AGREE_SHIP_POSTAL', 'AGREE_SHIP_STATE', 'AGREE_SHIP_CONTACT_EMAIL', 'AGREE_SHIP_CONTACT_NAME', 'AGREE_SHIP_CONTACT_PHONE', 'AGREE_SHIP_PGS_CONTACT_EMAIL', 'AGREE_SHIP_PGS_CONTACT_FIRST', 'AGREE_SHIP_PGS_CONTACT_ID', 'AGREE_SHIP_PGS_CONTACT_LAST', 'AGREE_SHIP_PGS_CONTACT_PHONE', 'ASSET_OWNER_ACCNT_NAME', 'ASSET_OWNER_ID', 'ASSET_OWNER_ADDRESS_1', 'ASSET_OWNER_ADDRESS_2', 'ASSET_OWNER_ADDRESS_ID', 'ASSET_OWNER_CITY', 'ASSET_OWNER_COUNTRY', 'ASSET_OWNER_POSTAL', 'ASSET_OWNER_STATE', 'ASSET_CONTACT_EMAIL', 'ASSET_CONTACT_FIRST', 'ASSET_CONTACT_ID', 'ASSET_CONTACT_LAST', 'ASSET_CONTACT_PHONE', 'ASSET_STATUS', 'POLYCOM_TERRITORY', 'PORTAL_PRIMARY_AGREE_NAME', 'PORTAL_PRIMARY_AGREE_ID'
FROM
	DUAL
UNION ALL
SELECT DISTINCT
	'Muliple Offers Matched',
	e.AGREE_ID, e.AGREE_NAME, e.AGREE_NUM, e.AGREE_PO, e.AGREE_SALES_REGION, e.THEATER, e.AGREE_SO, e.NO_OF_ASSET_ENTL_ON_LINE, e.AGREE_LINE_ID, e.AGREE_LINE_NUM, e.AGREE_LINE_PART_LIST, e.AGREE_LINE_PART_NET, e.AGREE_LINE_PART_NET_PER_ASSET, e.ANNUALIZED_VALUE, e.AGREE_LINE_QTY, e.AGREE_LINE_CURRENCY, e.MSRP, e.MKTG_NAME, e.DISCOUNT_CODE, e.BUNDLE_PART_TRNS, e.AGREE_LINE_SERVICE_PART_NUM, e.PIM_DESCRIPTION, e.ASSET_NUM, e.ASSET_ID, e.ASSET_PO_NUM, e.ASSET_SERIAL_NUM, e.ASSET_SERVICE_REGION, e.ASSET_SHIP_DATE, e.ASSET_SHIP_YEAR, e.ASSET_SO_NUM, e.ASSET_ADDRESS_1, e.ASSET_ADDRESS_2, e.ASSET_ADDRESS_ID, e.ASSET_CITY, e.ASSET_COUNTRY, e.ASSET_POSTAL, e.ASSET_STATE, e.ASSET_PART_NUM, e.ASSET_PRODUCT, e.ASSET_PRODUCT_DIVISION, e.ASSET_PRODUCT_GROUP, e.ASSET_MASTER_PRODUCT_GROUP, e.EOSL, e.ASSET_PRODUCT_ID, e.ASSET_PRODUCT_LINE, e.ENTL_CREATE_DATE, e.ENTL_DELIVERY_TYPE, e.ENTL_END_DATE, e.ENTL_BATCH_QUARTER, e.ENTL_ID, e.ENTL_NAME, e.ENTL_NET_PRICE_PER_ASSET, e.ENTL_NET_PRICE_PER_ASSET_USD, e.ENTL_SERVICE_TYPE, e.ENTL_ST_DATE, e.PREV_AGREE_ID, e.PREV_ENTL_END, e.PREV_ENTL_PO, e.PREV_ENTL_SRVC_PART_NUM, e.PREV_ENTITLEMENT_ID, e.SO_CURRENCY, e.SO_DATE, e.SO_EXT_NET_PRICE, e.SO_EXT_NET_PRICE_USD, e.AGREE_ACCNT_ID, e.AGREE_ACCNT_NAME, e.AGREE_MASTER_ACCNT_ID, e.AGREE_MSTR_ACCNT_NAME, e.AGREE_ACCNT_GAN, e.AGREE_ACCNT_ADDRESS_1, e.AGREE_ACCNT_ADDRESS_2, e.AGREE_ACCNT_ADDRESS_ID, e.AGREE_ACCNT_CITY, e.AGREE_ACCNT_COUNTRY, e.AGREE_ACCNT_POSTAL, e.AGREE_ACCNT_STATE, e.AGREE_ACCNT_CONTACT_EMAIL, e.AGREE_ACCNT_CONTACT_NAME, e.AGREE_ACCNT_CONTACT_PHONE, e.AGREE_ACCNT_PGS_CONTACT_EMAIL, e.AGREE_ACCNT_PGS_CONTACT_FIRST, e.AGREE_ACCNT_PGS_CONTACT_ID, e.AGREE_ACCNT_PGS_CONTACT_LAST, e.AGREE_ACCNT_PGS_CONTACT_PHONE, e.AGREE_END_CUST_ID, e.AGREE_END_CUST_MSTR_ID, e.AGREE_END_CUST_NAME, e.AGREE_END_CUST_MSTR_NAME, e.AGREE_END_CUST_ADDRESS_1, e.AGREE_END_CUST_ADDRESS_2, e.AGREE_END_CUST_ADDRESS_ID, e.AGREE_END_CUST_CITY, e.AGREE_END_CUST_COUNTRY, e.AGREE_END_CUST_POSTAL, e.AGREE_END_CUST_STATE, e.AGREE_END_CUST_CONTACT_EMAIL, e.AGREE_END_CUST_CONTACT_NAME, e.AGREE_END_CUST_CONTACT_PHONE, e.AGREE_END_CUST_PGS_CONTACT_EMAIL, e.AGREE_END_CUST_PGS_CONTACT_FIRST, e.AGREE_END_CUST_PGS_CONTACT_ID, e.AGREE_END_CUST_PGS_CONTACT_LAST, e.AGREE_END_CUST_PGS_CONTACT_PHONE, e.AGREE_RES_MASTER_ACCNT_ID, e.AGREE_RES_ACCNT_NAME, e.AGREE_RES_ACCT_MSTR_NAME, e.AGREE_RES_ID, e.AGREE_RES_ADDRESS_1, e.AGREE_RES_ADDRESS_2, e.AGREE_RES_ADDRESS_ID, e.AGREE_RES_CITY, e.AGREE_RES_COUNTRY, e.AGREE_RES_POSTAL, e.AGREE_RES_STATE, e.AGREE_RES_CONTACT_EMAIL, e.AGREE_RES_CONTAC_NAME, e.AGREE_RES_CONTACT_PHONE, e.AGREE_RES_PGS_CONTACT_EMAIL, e.AGREE_RES_PGS_CONTACT_FIRST, e.AGREE_RES_PGS_CONTACT_ID, e.AGREE_RES_PGS_CONTACT_LAST, e.AGREE_RES_PGS_CONTACT_PHONE, e.AGREE_SHIP_ACCNT_NAME, e.AGREE_SHIP_ID, e.AGREE_SHIP_ADDRESS_1, e.AGREE_SHIP_ADDRESS_2, e.AGREE_SHIP_ADDRESS_ID, e.AGREE_SHIP_CITY, e.AGREE_SHIP_COUNTRY, e.AGREE_SHIP_POSTAL, e.AGREE_SHIP_STATE, e.AGREE_SHIP_CONTACT_EMAIL, e.AGREE_SHIP_CONTACT_NAME, e.AGREE_SHIP_CONTACT_PHONE, e.AGREE_SHIP_PGS_CONTACT_EMAIL, e.AGREE_SHIP_PGS_CONTACT_FIRST, e.AGREE_SHIP_PGS_CONTACT_ID, e.AGREE_SHIP_PGS_CONTACT_LAST, e.AGREE_SHIP_PGS_CONTACT_PHONE, e.ASSET_OWNER_ACCNT_NAME, e.ASSET_OWNER_ID, e.ASSET_OWNER_ADDRESS_1, e.ASSET_OWNER_ADDRESS_2, e.ASSET_OWNER_ADDRESS_ID, e.ASSET_OWNER_CITY, e.ASSET_OWNER_COUNTRY, e.ASSET_OWNER_POSTAL, e.ASSET_OWNER_STATE, e.ASSET_CONTACT_EMAIL, e.ASSET_CONTACT_FIRST, e.ASSET_CONTACT_ID, e.ASSET_CONTACT_LAST, e.ASSET_CONTACT_PHONE, e.ASSET_STATUS, e.POLYCOM_TERRITORY, e.PORTAL_PRIMARY_AGREE_NAME, e.PORTAL_PRIMARY_AGREE_ID
FROM
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY e
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON e.TMP_ID = edat.TMP_ID
	AND edat.MATCH_STATUS = 'Multiple Offers Matched'
WHERE
	e.SS_STATUS = 'Not Processed'

INTO OUTFILE '${EX_DIR}/polycom_entitlement_exception_multiple_offers_matched_${F_SUFFIX}.csv'
	CHARACTER SET utf8
	FIELDS TERMINATED BY ','
	ENCLOSED BY '\"'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY '\n'
;

/*
**	Update only the 'Matched' records as Processed.
**	This will prevent us from reprocessing these records again.
*/
SET autocommit=0;
UPDATE
	polycom_data.ENTITLEMENT_DAILY_ACTIVITY e
INNER JOIN polycom_tmp.entitlement_daily_activity_tmp edat
	ON e.TMP_ID = edat.TMP_ID
	AND e.SS_STATUS = 'Not Processed'
	AND edat.MATCH_STATUS = 'Matched'
SET
	e.SS_STATUS = 'Processed';

UPDATE
	polycom_data.DELIVERY_OF_DAILY_ORDERS_ACTIVITY d
INNER JOIN polycom_tmp.agree_po_tmp apt
	ON d.AGREE_PO = apt.AGREE_PO
	AND d.SS_STATUS = 'Not Processed'
SET
	d.SS_STATUS = 'Processed';
commit;

SELECT NOW(), ' Polycom resolve as win script completed' from DUAL;
"
