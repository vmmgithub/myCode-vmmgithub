DELIMITER ;;
CREATE DEFINER=`gpolitano`@`10.100.%.%` PROCEDURE `step3_scrubRenewData_sp`(
IN stepId int
)
scrubRenewData:
BEGIN

/****************************************************************************

** Name:    step3_scrubRenewData_sp
** Desc:    Complete the processing with an final export of the data that needs to be modified in renew.
**				Bluecoat has 7 main scrub actions. CancelBooking. RevertOppToQuote. UpdateOffer. UpdateOpp. SplitOpp. CloseOpp. TagAsset. AddAsset.
**				HAOpp (seems to just pull from Renew data)
** Auth:    Grace Politano
** Date:    July 21, 2014
** Exec:    CALL bluecoat_bookings.step3_scrubRenewData_sp(1)
**************************
** Change History
**************************
** PR   Date	    Author   Description	
** --   --------   -------   ------------------------------------
** 

*****************************************************************************/
/*  Declare variables for status and error logging/reporting */

DECLARE step DECIMAL(10,1);
DECLARE code CHAR(5) DEFAULT '00000'; 
DECLARE msg TEXT;
DECLARE result TEXT;
DECLARE count BIGINT;

/* Declare exception handler for failed steps */

DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
	BEGIN 
		GET DIAGNOSTICS CONDITION 1
			code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
	END;

SET stepId = (SELECT IFNULL(stepId, 1));
SET @LoadDate = Now();

SET step = 1;

IF stepId <= step THEN

/* -------------------------------------------------------------------------
 *		Create a new Load record for this set of scrubs 
 * ------------------------------------------------------------------------- */

INSERT INTO bluecoat_bookings.RenewScrubLoad (loadDateTime,status) VALUES (@LoadDate, 'DataPrep'); -- Insert a new loadId, set the load to DataPrep

SET @LoadId = (SELECT LAST_INSERT_ID());

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Created new RenewScrubLoad Record ID = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to created new RenewScrubLoad Record error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

END IF;

/*
SET step = 2;

IF stepId <= step THEN
*/
/* -------------------------------------------------------------------------
 *		Insert new data into the subordinate OppSubordinateQuoteKey 
 *		This data was not necessary for matching but could be for scrubs
 * ------------------------------------------------------------------------- */
/*
-- Truncate the table to prepare for new data
TRUNCATE TABLE bluecoat_bookings.OppSubordinateQuoteKey;

		IF code = '00000' THEN

			SET count = NULL;
			SET result = CONCAT('Truncated table OppSubordinateQuoteKey as prep for new data Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to truncate table OppSubordinateQuoteKey as prep for new data Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 2.1;

-- Insert the 'new' base records from the renew data
INSERT INTO bluecoat_bookings.OppSubordinateQuoteKey
SELECT DISTINCT
				opps._ID AS oppUID,
  				opps.DISPLAYNAME AS oppName,
  				contact.DESTNAME AS company,
  				opps.FLOWS_SALESSTAGES_STATE_NAME as salesStage,
  				primaryQuote.DESTKEY AS useQuoteUID,
  				opps.AMOUNT_AMOUNT AS amount,
  				opps.AMOUNT_CODE_NAME AS currency,
  				NULL, -- booking.DESTKEY AS bookingUID,
  				primaryQuote.DESTKEY AS primaryQuoteUID,
  				NULL, -- latestQuote.DESTKEY AS latestQuoteUID,
				baseQuote.DESTKEY AS baseQuoteUID,
  				opps.SYSTEMPROPERTIES_CREATEDON AS createdOn
FROM			bluecoat.APP_OPPORTUNITIES opps
LEFT JOIN 		bluecoat.RELATIONSHIPS primaryQuote	ON primaryQuote.SOURCETABLE = 'APP_OPPORTUNITIES'
	AND 	primaryQuote.DESTTABLE = 'APP_QUOTES'
    AND 	primaryQuote.SOURCEKEY = opps._ID
    AND 	primaryQuote.RELNAME = 'primaryQuote'
LEFT JOIN 		bluecoat.RELATIONSHIPS baseQuote ON baseQuote.SOURCETABLE = 'APP_OPPORTUNITIES'
    AND 	baseQuote.DESTTABLE = 'APP_QUOTES'
    AND 	baseQuote.SOURCEKEY = opps._ID
    AND 	baseQuote.RELNAME = 'baseQuote'
LEFT JOIN 		bluecoat.RELATIONSHIPS contact ON contact.SOURCETABLE = 'APP_OPPORTUNITIES'
    AND 	contact.DESTTABLE = 'CORE_CONTACTS'
    AND 	contact.SOURCEKEY = opps._ID
    AND 	contact.RELNAME = 'customer'
WHERE		opps.ISSUBORDINATE = 'true'
	AND 	opps.FLOWS_SALESSTAGES_STATE_NAME NOT IN ('transitioned','consolidated')
	AND 	opps.DISPLAYNAME NOT LIKE '%Transitioned%' ;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Insert records into OppSubordinateQuoteKey as prep for new data Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to Insert records into OppSubordinateQuoteKey as prep for new data Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 2.2;

-- Update the records for the latestQuote & useQuote value (if null)
UPDATE bluecoat_bookings.OppSubordinateQuoteKey opps
JOIN   bluecoat_bookings.latestQuote latestQuote ON latestQuote.sourceTable = 'APP_OPPORTUNITIES'
    AND 	latestQuote.DESTTABLE = 'APP_QUOTES'
    AND 	latestQuote.SOURCEKEY = opps.oppUID
    AND 	latestQuote.RELNAME = 'quote'
SET    opps.latestQuoteUID = latestQuote.DESTKEY;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in OppSubordinateQuoteKey as prep for new data Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in OppSubordinateQuoteKey as prep for new data Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 2.3;

-- Update the records for the bookingUID & useQuote value (if null)
UPDATE bluecoat_bookings.OppSubordinateQuoteKey opps
JOIN   bluecoat_bookings.ValidBooking booking ON booking.sourceTable = 'APP_OPPORTUNITIES'
    AND 	booking.destTable = 'APP_BOOKINGS'
    AND 	booking.sourceKey = opps.oppUID
SET    opps.bookingUID = booking.DESTKEY;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in OppSubordinateQuoteKey as prep for new data Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in OppSubordinateQuoteKey as prep for new data Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 2.4;

-- Update the records for the bookingUID & useQuote value (if null)
UPDATE bluecoat_bookings.OppSubordinateQuoteKey opps
SET    opps.useQuoteUID = IFNULL( IFNULL( opps.useQuoteUID, opps.latestQuoteUID ), opps.baseQuoteUID )
WHERE  opps.useQuoteUID IS NULL;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in OppSubordinateQuoteKey as prep for new data Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in OppSubordinateQuoteKey as prep for new data Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;
END IF;
*/
SET step = 3;

IF stepId <= step THEN

/* -------------------------------------------------------------------------
 *		CloseOpps
 * ------------------------------------------------------------------------- */
INSERT INTO bluecoat_bookings.CloseOpps (loadId, loadDateTime, OppId, resolutionDate, poAmount, poDate, poNumber, reason, soAmount, soDate, soNumber)
SELECT
			@LoadId AS loadId,
			@LoadDate AS loadDateTime,
			_id AS OppId,
			resolutionDate,
			poAmount,
			poDate,
			poNumber,
			reason,
			soAmount,
			soDate,
			soNumber
FROM		bluecoat_bookings.BluecoatSetCloseOpp;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in CloseOpps as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in CloseOpps as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 3.1;

  IF count > 0 THEN

  INSERT INTO bluecoat_bookings.ScrubRenewData (loadId, loadDate, dataScrub, _id, collection, dataValues, isProcessed )
  SELECT 	loadId,
			loadDateTime,
		  	'CloseOpps',
		  	OppId AS _id,
			'Opportunities' AS collection,
	  		CONCAT('"',OppId,'","',resolutionDate,'","',poAmount,'","',poDate,'","',poNumber,'","',reason,'","',soAmount,'","',soDate,'","',soNumber,'"'),
	  		0
  FROM		bluecoat_bookings.CloseOpps
  WHERE		loadId = @LoadId;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Inserted the CloseOpps records into ScrubRenewData for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to insert the CloseOpps records into ScrubRenewData for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

   END IF;

END IF;

SET step = 4;

IF stepId <= step THEN

/* -------------------------------------------------------------------------
 *
 *		RevertOppToQuote
 *
 * ------------------------------------------------------------------------- */
INSERT INTO bluecoat_bookings.RevertOppsToQuote (loadId, loadDateTime, OppId)
SELECT
			@LoadId AS loadId,
			@LoadDate AS loadDateTime,
			oppUID AS OppId
FROM        bluecoat_bookings.BluecoatSetRevertOppToQuote;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in RevertOppsToQuote as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in RevertOppsToQuote as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 4.1;

  IF count > 0 THEN

  INSERT INTO bluecoat_bookings.ScrubRenewData (loadId, loadDate, dataScrub, _id, collection, dataValues, isProcessed )
  SELECT 	loadId,
			loadDateTime,
		  	'RevertOppsToQuote',
		  	OppId AS _id,
			'Opportunities' AS collection,
	  		CONCAT('"',OppId,'"'),
	  		0
  FROM		bluecoat_bookings.RevertOppsToQuote
  WHERE		loadId = @LoadId;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Inserted the RevertOppsToQuote records into ScrubRenewData for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to insert the RevertOppsToQuote records into ScrubRenewData for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

   END IF;

END IF;

SET step = 5;

IF stepId <= step THEN
/* -------------------------------------------------------------------------
 *		SplitOpps
 * ------------------------------------------------------------------------- */
INSERT INTO bluecoat_bookings.SplitOpps (loadId, loadDateTime, OppId, OfferIds)
SELECT
			@LoadId AS loadId,
			@LoadDate AS loadDateTime,
			oppUID AS OppId,
			Move_Offers AS OfferIds
FROM        bluecoat_bookings.BluecoatSetSplitOpp;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in SplitOpps as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in SplitOpps as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 5.1;

  IF count > 0 THEN

  INSERT INTO bluecoat_bookings.ScrubRenewData (loadId, loadDate, dataScrub, _id, collection, dataValues, isProcessed )
  SELECT 	loadId,
			loadDateTime,
		  	'SplitOpps',
		  	OppId AS _id,
			'Opportunities' AS collection,
	  		CONCAT('"',OppId,'","',OfferIds,'"'),
	  		0
  FROM		bluecoat_bookings.SplitOpps
  WHERE		loadId = @LoadId;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Inserted the SplitOpps records into ScrubRenewData for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to insert the SplitOpps records into ScrubRenewData for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

   END IF;

END IF;

SET step = 6;

IF stepId <= step THEN
/* -------------------------------------------------------------------------
 *		UpdateOpps
 * ------------------------------------------------------------------------- */
-- UpdateOpps set renew scrub for Amount & Date 
INSERT INTO bluecoat_bookings.UpdateOpps (loadId, loadDateTime, OppId, Amount, currency, expirationDate)
SELECT DISTINCT 
			@LoadId AS loadId,
			@LoadDate AS loadDateTime,
			book.oppUID AS OppId,
			CASE
				WHEN book.offerDESC LIKE '%Amount%' THEN ROUND(SUM(IFNULL(bsum.Value,0)),2) 
			END AS NewOppAmount,
			CASE
				WHEN book.offerDESC LIKE '%Amount%' THEN 'usd' 
			END AS NewCurrency,
			CASE 
				WHEN book.offerDESC LIKE '%Date%'  THEN DATE_SUB(MIN(bsum.ServiceStart),INTERVAL 1 DAY) 
			END AS NewExpirationDate
FROM  		bluecoat_bookings.BluecoatBookingFile book
JOIN		bluecoat_bookings.BluecoatBookingSummary bsum
		ON  bsum.`OrderNumber` = book.`OrderNumber`
		AND bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber` 
JOIN 		bluecoat_bookings.OfferQuoteKey offer
		ON offer.offerUID = book.offerUID 
JOIN		bluecoat_bookings.OppQuoteKey opp
		ON  book.oppUID = opp.oppUID    
WHERE      book.isComplete = 0
		AND book.offerUID IS NOT NULL 
		AND book.offerDESC <> 'exact'
GROUP BY book.oppUID;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in UpdateOpps as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in UpdateOpps as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 6.1;
 
-- UpdateOpps set TargetSelling Period
INSERT INTO bluecoat_bookings.UpdateOpps (loadId, loadDateTime, OppId, targetSellingPeriod)
SELECT   @LoadId AS loadId,
		 @LoadDate AS loadDateTime,
		 tsp.oppUID AS OppId,
		 tsp.targetSellingPeriod 
FROM     bluecoat_bookings.BluecoatOppTargetSellingPeriod AS tsp
WHERE    targetSellingPeriod <> extensions_master_targetperiod_value_name
ON DUPLICATE KEY UPDATE 
		 loadDateTime = @LoadDate,
		 targetSellingPeriod = tsp.targetSellingPeriod;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in UpdateOpps as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to update the records in UpdateOpps as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 6.2;

  IF count > 0 THEN

  INSERT INTO bluecoat_bookings.ScrubRenewData (loadId, loadDate, dataScrub, _id, collection, dataValues, isProcessed )
  SELECT 	loadId,
			loadDateTime,
		  	'UpdateOpps',
		  	OppId AS _id,
			'Opportunities' AS collection,
	  		CONCAT('"',IFNULL(OppId,''),'","',IFNULL(Amount,''),'","',IFNULL(currency,''),'","',IFNULL(expirationDate,''),'","',IFNULL(targetSellingPeriod,''),'"'),
	  		0
  FROM		bluecoat_bookings.UpdateOpps
  WHERE		loadId = @LoadId;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Inserted the UpdateOpps records into ScrubRenewData for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to insert the UpdateOpps records into ScrubRenewData for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

   END IF;

END IF;

SET step = 7;

IF stepId <= step THEN
/* -------------------------------------------------------------------------
 *		UpdateOffers
 * ------------------------------------------------------------------------- */
-- UpdateOffers change the Amounts & Dates where necessary -- output to CSV
INSERT INTO bluecoat_bookings.UpdateOffers(loadId, loadDateTime, OfferId, Amount, currency, startDate, endDate)
SELECT DISTINCT   
				  @LoadId AS loadId,
				  @LoadDate AS loadDateTime,
				  book.offerUID AS OfferId,
				  CASE 
					WHEN book.offerDESC LIKE '%Amount%' THEN ROUND(IFNULL(bsum.Value,0),2) 
					ELSE NULL
				  END AS New_OfferAmount,
				  CASE 
					WHEN book.offerDESC LIKE '%Amount%' THEN 'usd' 
					ELSE NULL
				  END AS New_Currency,
				  CASE 
					WHEN book.offerDESC  LIKE '%Date%' AND DATE(offer.offerStart) <> DATE(bsum.ServiceStart) THEN bsum.ServiceEnd 
					ELSE NULL
				  END AS New_StartDate,
				  CASE 
					WHEN book.offerDESC  LIKE '%Date%' AND DATE(offer.offerEnd) <> DATE(bsum.ServiceEnd) THEN bsum.ServiceEnd 
					ELSE NULL
				  END AS New_EndDate
FROM  		bluecoat_bookings.BluecoatBookingFile book
JOIN		bluecoat_bookings.BluecoatBookingSummary bsum
		ON  bsum.`OrderNumber` = book.`OrderNumber`
		AND bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber` 
JOIN 		bluecoat_bookings.OfferQuoteKey offer
		ON offer.offerUID = book.offerUID 
JOIN		bluecoat_bookings.OppQuoteKey opp
		ON  book.oppUID = opp.oppUID 
WHERE      isComplete = 0
		AND book.offerUID IS NOT NULL 
		AND book.offerDESC <> 'exact' 
ORDER BY book.offerUID; 

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in UpdateOffers as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in UpdateOffers as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 7.1;

-- UpdateOffers change the isExcluded value to 'undefined'
INSERT INTO bluecoat_bookings.UpdateOffers(loadId, loadDateTime, OfferId, isExcluded)
SELECT DISTINCT 
				  @LoadId AS loadId,
				  @LoadDate AS loadDateTime,
				  ie.OfferId,
				  ie.is_excluded AS isExcluded
FROM 			bluecoat_bookings.BluecoatSetOfferIsExcluded AS ie
ON DUPLICATE KEY UPDATE 
				loadDateTime = @LoadDate,
				isExcluded = ie.is_excluded;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in UpdateOffers as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in UpdateOffers as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 7.2;

  IF count > 0 THEN

  INSERT INTO bluecoat_bookings.ScrubRenewData (loadId, loadDate, dataScrub, _id, collection, dataValues, isProcessed )
  SELECT 	loadId,
			loadDateTime,
		  	'UpdateOffers',
		  	OfferId AS _id,
			'Offers' AS collection,
	  		CONCAT('"',IFNULL(OfferId,''),'","',IFNULL(Amount,''),'","',IFNULL(currency,''),'","',IFNULL(startDate,''),'","',IFNULL(endDate,''),'","',IFNULL(isExcluded,''),'"'),
	  		0
  FROM		bluecoat_bookings.UpdateOffers
  WHERE		loadId = @LoadId;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Inserted the UpdateOffers records into ScrubRenewData for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to insert the UpdateOffers records into ScrubRenewData for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

   END IF;

END IF;

SET step = 8;

IF stepId <= step THEN
/* -------------------------------------------------------------------------
 *		CancelBookings
 * ------------------------------------------------------------------------- */
INSERT INTO bluecoat_bookings.CancelBookings(loadId, loadDateTime,BookingId)
SELECT			 @LoadId AS loadId,
				 @LoadDate AS loadDateTime,
				 BookingUID AS BookingId
FROM 		bluecoat_bookings.BluecoatSetCancelBookings
WHERE 		BookingUID IS NOT NULL;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in CancelBookings as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in CancelBookings as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 8.1;

  IF count > 0 THEN

  INSERT INTO bluecoat_bookings.ScrubRenewData (loadId, loadDate, dataScrub, _id, collection, dataValues, isProcessed )
  SELECT 	loadId,
			loadDateTime,
		  	'CancelBookings',
		  	BookingId AS _id,
			'Bookings' AS collection,
	  		CONCAT('"',BookingId,'"'),
	  		0
  FROM		bluecoat_bookings.CancelBookings
  WHERE		loadId = @LoadId;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Inserted the CancelBookings records into ScrubRenewData for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to insert the CancelBookings records into ScrubRenewData for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

   END IF;

END IF;

SET step = 9;

IF stepId <= step THEN
/* -------------------------------------------------------------------------
 *
 *		TagAssets
 *
 * ------------------------------------------------------------------------- */
INSERT INTO bluecoat_bookings.TagAssets (loadId, loadDateTime, AssetId, Tag)
SELECT			 @LoadId AS loadId,
				 @LoadDate AS loadDateTime,
				 _ID AS AssetId,
				 CAST(@LoadDate AS DATE) AS Tag
FROM		bluecoat_bookings.BluecoatSetAssetTag;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in TagAssets as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in TagAssets as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

SET step = 9.1;

  IF count > 0 THEN

  INSERT INTO bluecoat_bookings.ScrubRenewData (loadId, loadDate, dataScrub, _id, collection, dataValues, isProcessed )
  SELECT 	loadId,
			loadDateTime,
		  	'TagAssets',
		  	AssetId AS _id,
			'Assets' AS collection,
	  		CONCAT('"',AssetId,'","',Tag,'"'),
	  		0
  FROM		bluecoat_bookings.TagAssets
  WHERE		loadId = @LoadId;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Inserted the TagAssets records into ScrubRenewData for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;

		ELSE

			SET result = CONCAT('Failed to insert the TagAssets records into ScrubRenewData for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;

   END IF;

END IF;

SET step = 10;

IF stepId <= step THEN
/* -------------------------------------------------------------------------
 *
 *		AddAssets
 *
 * ------------------------------------------------------------------------- */
INSERT INTO bluecoat_bookings.AddAssets (`loadId`,`loadDateTime`,`clienttheatre`,`country`,`EndUserCompanyOpportunity`,`ExistingSerialNumber`,`ExistingServiceProduct`,`CoveredProduct`,
											`BatchType`,`SSIBusinessLine`,`ExistingEndDate`,`resolutionDate`,`poAmount`,`PurchaseOrderNumber`,`poDate`,`reason`,`SOAmount`,`soDate`,
											`SONumber`,`TargetSellingPeriod`,`LocalAmount`,`LocalCurrency`,`DISPLAYNAME`,`relname`)

SELECT			  @LoadId AS loadId,
				  @LoadDate AS loadDateTime,
				 `clienttheatre`,
				 `country`,
				 `End User Company (Opportunity)` EndUserCompanyOpportunity,
				 `Existing Serial Number` ExistingSerialNumber,
				 `Existing Service Product` ExistingServiceProduct,
				 `Covered Product` CoveredProduct,
				 `Batch Type` BatchType,
				 `SSI Business Line` SSIBusinessLine,
				 `Existing End Date`,
				 `resolutionDate`,
				 `poAmount`,
				 `poNumber`,
				 `poDate`,
				 `reason`,
				 `SO Amount`,
				 `soDate`,
				 `SO Number`,
				 `Target Selling Period`,
				 `Local Amount`,
				 `Local Currency`,
				 `DISPLAYNAME`,
				 `relname`
FROM 		`bluecoat_bookings`.`BluecoatAddAssets`;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Updated the records in AddAssets as prep for renew data scrub Load = ', @LoadId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, count);
			SELECT NOW(), 'scrubRenewData_sp', step, result, count;
			UPDATE RenewScrubLoad SET status = 'DataReady' WHERE loadId = @LoadId;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update the records in AddAssets as prep for renew data scrub Load = ', @LoadId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'scrubRenewData_sp', step, result, '');
			SELECT NOW(), 'scrubRenewData_sp', step, result, '';
			LEAVE scrubRenewData;

		END IF;
END IF;


END;;
DELIMITER ;