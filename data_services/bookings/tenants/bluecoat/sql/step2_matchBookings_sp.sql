DELIMITER ;;
CREATE DEFINER=`gpolitano`@`10.100.%.%` PROCEDURE `step2_matchBookings_sp`(
IN stepId int
)
matchBookings:
BEGIN

/****************************************************************************

** Name:    step2_matchBookings_sp
** Desc:    Import the 'new' data from the stage tables and insert into the bluecoat_bookings.BluecoatBookingFile.
**				Reprocess any bookings not previously completed, Use matching logic to process the new bookings.
**				Set booking to 'split' or close based on processing. Prepare values to be scrubbed.
** Auth:    Grace Politano
** Date:    July 14, 2014
** Exec:    CALL bluecoat_bookings.step2_matchBookings_sp(8)
**************************
** Change History
**************************
** PR   Date	    Author   Description	
** --   --------   -------   ------------------------------------
** 1	2014-07-18  GMP 	  Added 'restart' points into the sp at steps 

*****************************************************************************/
/*  Declare variables for status and error logging/reporting */

DECLARE step INT;
DECLARE code CHAR(5) DEFAULT '00000'; 
DECLARE msg TEXT;
DECLARE result TEXT;
DECLARE count BIGINT;
DECLARE dupcount BIGINT;

/* Declare exception handler for failed steps */

DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
	BEGIN 
		GET DIAGNOSTICS CONDITION 1
			code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
	END;

SET stepId = (SELECT IFNULL(stepId, 1));

IF stepId <= step THEN

	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'stored procedure started', '');
	SELECT NOW(), 'matchBookings_sp', step, 'stored procedure started', '';

END IF;
/* Adding in if step logic to allow the procedure to be run from failure or 'restart' points */
/*
IF stepId <= step THEN

-- Truncate the existing records

TRUNCATE TABLE bluecoat_bookings.OfferQuoteKey;

	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'OfferQuoteKey table truncated', '');
	SELECT NOW(), 'matchBookings_sp', step, 'OfferQuoteKey table truncated', '';


TRUNCATE TABLE bluecoat_bookings.OppQuoteKey;

	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'OppQuoteKey table truncated', '');
	SELECT NOW(), 'matchBookings_sp', step, 'OppQuoteKey table truncated', '';
*/
/* Pull the new Renew data into a format to use for the matching logic for the BluecoatBookingFile table */
/*	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'processing of dependant Renew data started', '');
	SELECT NOW(), 'matchBookings_sp', step, 'processing of dependant Renew data started', '';

INSERT INTO bluecoat_bookings.OfferQuoteKey
SELECT DISTINCT
				offers._ID AS offerUID,
				offers.DISPLAYNAME AS offerName,
				offers.RESULT_NAME AS resultName,
				product.DESTNAME AS product,
				quotes.DESTKEY AS quoteUID,
				offers.amount_amount AS offerAmount,
				offers.amount_code_name AS offerCurrency,
				offers.startdate AS offerStart,
				offers.enddate AS offerEnd,
				predecessor.DESTKEY AS predecessorUID,
				offers.ISEXCLUDED AS isExcluded
FROM			bluecoat.APP_OFFERS offers
LEFT JOIN 		bluecoat.RELATIONSHIPS product ON product.SOURCETABLE = 'APP_OFFERS'
			AND product.DESTTABLE = 'APP_PRODUCTS'
			AND product.SOURCEKEY = offers._ID
LEFT JOIN 		bluecoat.RELATIONSHIPS quotes  ON quotes.SOURCETABLE = 'APP_OFFERS'
			AND quotes.DESTTABLE = 'APP_QUOTES'  
			AND quotes.SOURCEKEY = offers._ID
LEFT JOIN 		bluecoat.RELATIONSHIPS predecessor ON predecessor.SOURCETABLE = 'APP_OFFERS'
			AND predecessor.DESTTABLE = 'APP_ASSETS'
			AND predecessor.RELNAME = 'predecessor'
			AND predecessor.SOURCEKEY = offers._ID;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'OfferQuoteKey new records inserted', count);
	SELECT NOW(), 'matchBookings_sp', step, 'OfferQuoteKey new records inserted', count;
	COMMIT;
ELSE

    SET result = CONCAT('new data failed to be created fpr OfferQuoteKey, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, result, '');
	SELECT NOW(), 'matchBookings_sp', step, result, '';
	LEAVE matchBookings;

END IF;

INSERT INTO bluecoat_bookings.OppQuoteKey
SELECT DISTINCT
				opps._ID AS oppUID,
  				opps.DISPLAYNAME AS oppName,
  				contact.DESTNAME AS company,
  				opps.FLOWS_SALESSTAGES_STATE_NAME as salesStage,
  				COALESCE(COALESCE(primaryQuote.DESTKEY,latestQuote.DESTKEY),baseQuote.DESTKEY) AS useQuoteUID,
  				opps.AMOUNT_AMOUNT AS amount,
  				opps.AMOUNT_CODE_NAME AS currency,
  				booking.DESTKEY AS bookingUID,
  				primaryQuote.DESTKEY AS primaryQuoteUID,
  				latestQuote.DESTKEY AS latestQuoteUID,
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
LEFT JOIN 		bluecoat_bookings.latestQuote latestQuote ON latestQuote.sourceTable = 'APP_OPPORTUNITIES'
    AND 	latestQuote.DESTTABLE = 'APP_QUOTES'
    AND 	latestQuote.SOURCEKEY = opps._ID
    AND 	latestQuote.RELNAME = 'quote'
LEFT JOIN 		bluecoat.RELATIONSHIPS contact ON contact.SOURCETABLE = 'APP_OPPORTUNITIES'
    AND 	contact.DESTTABLE = 'CORE_CONTACTS'
    AND 	contact.SOURCEKEY = opps._ID
    AND 	contact.RELNAME = 'customer'
LEFT JOIN 		bluecoat_bookings.ValidBooking booking ON booking.sourceTable = 'APP_OPPORTUNITIES'
    AND 	booking.destTable = 'APP_BOOKINGS'
    AND 	booking.sourceKey = opps._ID
WHERE		opps.ISSUBORDINATE <> 'true'
	AND 	opps.FLOWS_SALESSTAGES_STATE_NAME NOT IN ('transitioned','consolidated')
	AND 	opps.DISPLAYNAME NOT LIKE '%Transitioned%';

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'OppQuoteKey new records inserted', count);
	SELECT NOW(), 'matchBookings_sp', step, 'OppQuoteKey new records inserted', count;
	COMMIT;
ELSE

    SET result = CONCAT('new data failed to be created for OppQuoteKey, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, result, '');
	SELECT NOW(), 'matchBookings_sp', step, result, '';
	LEAVE matchBookings;

END IF;

END IF;
*/
SET step =  2;

/*   Refresh the values for loopId 3, just incase they have changed    */

DELETE FROM bluecoat_bookings.SalesStageLoop WHERE loopId = 3;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'Removed existing records for loopId 3 (any) from SalesStageLoop', count);
	SELECT NOW(), 'matchBookings_sp', step, 'Removed existing records for loopId 3 (any) from SalesStageLoop', count;

ELSE

    SET result = CONCAT('failed to remove existing records for loopId 3 (any) from SalesStageLoop, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, result, '');
	SELECT NOW(), 'matchBookings_sp', step, result, '';

END IF;

INSERT INTO bluecoat_bookings.SalesStageLoop
SELECT DISTINCT 3,  opp.salesstage
FROM 		bluecoat_bookings.OppQuoteKey opp;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'Inserted new records for loopId 3 (any) to SalesStageLoop', count);
	SELECT NOW(), 'matchBookings_sp', step, 'Inserted new records for loopId 3 (any) to SalesStageLoop', count;
	COMMIT;
ELSE

    SET result = CONCAT('failed to insert new records for loopId 3 (any) to SalesStageLoop, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, result, '');
	SELECT NOW(), 'matchBookings_sp', step, result, '';
	LEAVE matchBookings;

END IF;

SET step = 3;

IF stepId <= step THEN  -- Restart point

-- reset the records we should have booked, just in case they didn't
UPDATE 		bluecoat_bookings.BluecoatBookingFile book
SET			book.booked = NULL
WHERE       book.booked = 'next';

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'BluecoatBookingFile reset previous booking records from next to NULL', count);
	SELECT NOW(), 'matchBookings_sp', step, 'BluecoatBookingFile reset previous booking records from next to NULL', count;
	COMMIT;
ELSE

    SET result = CONCAT('failed to reset previous booking records from next to NULL in BluecoatBookingFile, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, result, '');
	SELECT NOW(), 'matchBookings_sp', step, result, '';
	LEAVE matchBookings;

END IF;

/*    Reset any prior booking which may have needed to be scrubbed after the initial run. Where the offerUID is populated and the isCompleted flag is 0  */

UPDATE 		bluecoat_bookings.BluecoatBookingFile book
LEFT JOIN	bluecoat_bookings.OfferQuoteKey origOffer
		ON origOffer.offerUID = book.offerUID
LEFT JOIN	bluecoat_bookings.OppQuoteKey opp
		ON opp.useQuoteUID = origOffer.QuoteUID
SET			book.offerDesc = 'moved'
WHERE		book.offerUID IS NOT NULL
		AND opp.oppUID IS NULL
		AND isComplete = 0; -- 0

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, 'BluecoatBookingFile reset previous booking records to be matched', count);
	SELECT NOW(), 'matchBookings_sp', step, 'BluecoatBookingFile reset previous booking records to be matched', count;
	COMMIT;
ELSE

    SET result = CONCAT('failed to reset previous booking records for BluecoatBookingFile table, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'matchBookings_sp', step, result, '');
	SELECT NOW(), 'matchBookings_sp', step, result, '';
	LEAVE matchBookings;

END IF;


SET step = 4;
/*  Now loop through based on sales stage to find the best(closed sale)/better(open sale)/good match (any stage) for the reset records and update the opp/offer to 'new' UIDs */

SET @loopId = 1;  
WHILE @loopId <= 3 DO

	UPDATE 		bluecoat_bookings.BluecoatBookingFile book
	JOIN        (SELECT 	book.OrderNumber, book.QuoteSerialNumber, opp.oppUID, newOffer.offerUID
				 FROM 		bluecoat_bookings.BluecoatBookingFile book
				 JOIN  		bluecoat_bookings.OfferQuoteKey origOffer
					ON 	origOffer.offerUID = book.offerUID
				 JOIN  		bluecoat_bookings.OfferQuoteKey newOffer  
					ON 	newOffer.predecessorUID = origOffer.predecessorUID
				 JOIN  		bluecoat_bookings.OppQuoteKey opp
					ON 	opp.useQuoteUID = newOffer.QuoteUID
				 JOIN	    bluecoat_bookings.SalesStageLoop stage
					ON 	opp.SalesStage = stage.stage AND stage.loopId = @loopId 
				 LEFT JOIN	bluecoat_bookings.BurnedOppsComplete burnedOpps
					ON burnedOpps.oppUID = opp.oppUID
				 WHERE   	book.offerUID IS NOT NULL
					AND   	newOffer.offerUID IS NOT NULL
					AND   	burnedOpps.oppUID IS NULL
					AND   	book.offerDesc = 'moved'
					AND   	newOffer.offerUID <> book.offerUID
					AND   	book.isComplete = 0
				ORDER BY opp.oppUID, newOffer.offerUID) oppOffer ON oppOffer.OrderNumber = book.OrderNumber AND oppOffer.QuoteSerialNumber = book.QuoteSerialNumber
	SET			book.oppUID = oppOffer.oppUID,
				book.offerUID = oppOffer.offerUID,
				book.offerDesc = NULL
	WHERE   	book.offerUID IS NOT NULL
			AND oppOffer.offeruid is not null
			AND book.offerDesc = 'moved'
			AND oppOffer.offeruid <> book.offeruid
			AND book.isComplete = 0; -- 1 = 0 2 = 0 3 = 0

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('matched previous booking records for BluecoatBookingFile table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;
		ELSE

			SET result = CONCAT('failed to reset previous booking records for BluecoatBookingFile tableloopId = ', @loopId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;

	SET @loopId = @loopId + 1;

END WHILE;

SET step = 5; -- 5

/*  Now for the reset records where the 'new' offerUID was set let's set the offerDesc to notate how good a match it is  */

UPDATE 		bluecoat_bookings.BluecoatBookingFile book
SET			book.offerDesc = NULL
WHERE		book.offerUID IS NOT NULL
		AND book.isComplete = 0; -- 0


		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = 'reset offerDesc to NULL for offerUID NOT NULL for BluecoatBookingFile table';
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;
		ELSE

			SET result = CONCAT('failed to reset offerDesc to NULL for offerUID NOT NULL for BluecoatBookingFile table, error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;

-- Now reset the description appropriately based on best matching values 

UPDATE 		bluecoat_bookings.BluecoatBookingFile book
JOIN		bluecoat_bookings.BluecoatBookingSummary bsum
		ON  bsum.`OrderNumber` = book.`OrderNumber`
		AND bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber`
JOIN		bluecoat_bookings.OfferQuoteKey offer
		ON  offer.offerUID = book.offerUID
SET			book.offerDesc = 
				CASE 
					WHEN bsum.ServiceEnd = DATE(offer.offerEnd) AND ROUND(bsum.value,0) = ROUND(offer.offerAmount,0) THEN 'Exact'
 					WHEN bsum.ServiceEnd <> DATE(offer.offerEnd) AND ROUND(bsum.value,0) = ROUND(offer.offerAmount,0) THEN 'Date off'
 					WHEN bsum.ServiceEnd = DATE(offer.offerEnd) AND ROUND(bsum.value,0) <> ROUND(offer.offerAmount,0) THEN 'Amount off'
 					WHEN bsum.ServiceEnd <> DATE(offer.offerEnd) AND ROUND(bsum.value,0) <> ROUND(offer.offerAmount,0) THEN 'Date/Amount off'
				END
WHERE		book.offerUID IS NOT NULL
		AND book.isComplete = 0; -- 0


		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = 'reset offerDesc values for offerUID NOT NULL for BluecoatBookingFile table';
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('failed to reset offerDesc values for offerUID NOT NULL for BluecoatBookingFile table, error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;
END IF;

SET step = 6; -- 6

IF stepId <= step THEN  -- Restart point

/*  Now for the New data in the table lets find the best / better / good matches base on Exact/Amount/Dates and SalesStage   */

-- 1.	Find Exact Matches
SET @loopId = 1; 
WHILE @loopId <= 3 DO

	UPDATE 		bluecoat_bookings.BluecoatBookingFile book
	JOIN        (SELECT 	book.OrderNumber, book.QuoteSerialNumber, opp.oppUID, offer.offerUID, offer.predecessorUID
				 FROM 		bluecoat_bookings.BluecoatBookingFile book
				 JOIN		bluecoat_bookings.BluecoatBookingSummary bsum
					ON  	bsum.`OrderNumber` = book.`OrderNumber`
					AND 	bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber`
				 JOIN		bluecoat_bookings.OfferQuoteKey offer
					ON  	offer.offerName = book.`QuoteSerialNumber`
					AND 	bsum.ServiceEnd = DATE(offer.offerEnd) 
					AND 	ROUND(bsum.Value,0) = ROUND(offer.offerAmount,0)
				 JOIN		bluecoat_bookings.OppQuoteKey opp
					ON  	opp.useQuoteUID = offer.QuoteUID
				 JOIN		bluecoat_bookings.SalesStageLoop stage
					ON 		opp.SalesStage = stage.stage AND stage.loopId = @loopId 
				 LEFT JOIN  bluecoat_bookings.BurnedOffers burnedOffers
					ON  	burnedOffers.offerUID = offer.offerUID 
				 LEFT JOIN  bluecoat_bookings.BurnedAssets burnedAssets
					ON  	burnedAssets.predecessorUID = offer.predecessorUID 
				 WHERE		book.isComplete = 0 
					AND 	book.offerUID IS NULL
					AND 	burnedOffers.offerUID IS NULL
					AND 	burnedAssets.predecessorUID IS NULL	
				ORDER BY 	opp.oppUID, offer.offerUID, offer.predecessorUID) oppOffer ON oppOffer.OrderNumber = book.OrderNumber AND oppOffer.QuoteSerialNumber = book.QuoteSerialNumber
	SET			book.offerUID = oppOffer.offerUID,
				book.oppUID = oppOffer.oppUID,
				book.predecessorUID = oppOffer.predecessorUID,
				book.offerDesc = 'Exact'
	WHERE		book.isComplete = 0 
			AND book.offerUID IS NULL; -- 1 = 1,085 2 = 2,298 3 = 3

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('exact matched booking records for BluecoatBookingFile table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('exact matched booking records for BluecoatBookingFile table for loopId = ', @loopId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;

		IF (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID) > 10 THEN 

			SET dupcount = (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID);
			SET result = CONCAT('DuplicateOfferUID exist in the BluecoatBookingFile table for exact loop = ', @loopId);

			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, 'DuplicateOfferUID exist in the BluecoatBookingFile table for exact loop';

			UPDATE bluecoat_bookings.BluecoatBookingFile book
			SET    book.offerDesc = NULL,
				   book.offerUID = NULL,
       		       book.oppUID = NULL,
				   book.predecessorUID = NULL
			WHERE	 book.isComplete = 0 
			AND EXISTS (SELECT dup.offerUID FROM bluecoat_bookings.DuplicateOfferUID dup WHERE dup.offerUID = book.offerUID)
			AND `OrderNumber` = (SELECT dup.`OrderNumber` FROM bluecoat_bookings.UniqueBookingRecords dup WHERE dup.offerUID = book.offerUID LIMIT 1);
 
			SET dupcount = ROW_COUNT();
			SET result = CONCAT('Exact Matched booking records removed duplicate offerId table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, result, dupcount;

		END IF;


     SET @loopId = @loopId + 1;

END WHILE;

SET step = 7; -- 7

-- 	Find matches where the Amount is off
SET @loopId = 1;  
WHILE @loopId <= 3 DO

	UPDATE 		bluecoat_bookings.BluecoatBookingFile book
	JOIN        (SELECT 	book.OrderNumber, book.QuoteSerialNumber, opp.oppUID, offer.offerUID, offer.predecessorUID
				 FROM 		bluecoat_bookings.BluecoatBookingFile book
				 JOIN		bluecoat_bookings.BluecoatBookingSummary bsum
					ON  	bsum.`OrderNumber` = book.`OrderNumber`
					AND 	bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber`
				 JOIN		bluecoat_bookings.OfferQuoteKey offer
					ON  	offer.offerName = book.`QuoteSerialNumber`
					AND 	bsum.ServiceEnd = DATE(offer.offerEnd) 
				 JOIN		bluecoat_bookings.OppQuoteKey opp
					ON  	opp.useQuoteUID = offer.QuoteUID
				 JOIN		bluecoat_bookings.SalesStageLoop stage
					ON 		opp.SalesStage = stage.stage AND stage.loopId = @loopId 
				 LEFT JOIN  bluecoat_bookings.BurnedOffers burnedOffers
					ON  	burnedOffers.offerUID = offer.offerUID 
				 LEFT JOIN  bluecoat_bookings.BurnedAssets burnedAssets
					ON  	burnedAssets.predecessorUID = offer.predecessorUID 
				 WHERE		book.isComplete = 0 
					AND 	book.offerUID IS NULL
					AND 	burnedOffers.offerUID IS NULL
					AND 	burnedAssets.predecessorUID IS NULL	
				ORDER BY 	opp.oppUID, offer.offerUID, offer.predecessorUID) oppOffer ON oppOffer.OrderNumber = book.OrderNumber AND oppOffer.QuoteSerialNumber = book.QuoteSerialNumber 
	SET			book.offerUID = oppOffer.offerUID,
				book.oppUID = oppOffer.oppUID,
				book.predecessorUID = oppOffer.predecessorUID,
				book.offerDesc = 'Amount off'
	WHERE		book.isComplete = 0 
			AND book.offerUID IS NULL; -- 1 = 32 2 = 640 3 = 0


		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Amount Off matched booking records for BluecoatBookingFile table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Amount Off matched booking records for BluecoatBookingFile table for loopId = ', @loopId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;

		IF (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID) > 10 THEN 

			SET dupcount = (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID);
			SET result = CONCAT('DuplicateOfferUID exist in the BluecoatBookingFile table for Amount loop = ', @loopId);

			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, 'DuplicateOfferUID exist in the BluecoatBookingFile table for Amount loop';

			UPDATE bluecoat_bookings.BluecoatBookingFile book
			SET    book.offerDesc = NULL,
				   book.offerUID = NULL,
       		       book.oppUID = NULL,
				   book.predecessorUID = NULL
			WHERE	 book.isComplete = 0 
			AND EXISTS (SELECT dup.offerUID FROM bluecoat_bookings.DuplicateOfferUID dup WHERE dup.offerUID = book.offerUID)
			AND `OrderNumber` = (SELECT dup.`OrderNumber` FROM bluecoat_bookings.UniqueBookingRecords dup WHERE dup.offerUID = book.offerUID LIMIT 1);
 -- 5/3/0
			SET dupcount = ROW_COUNT();
			SET result = CONCAT('Amount off Matched booking records removed duplicate offerId table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, result, dupcount;
    
		END IF;

     SET @loopId = @loopId + 1;

END WHILE;

SET step = 8; -- 8 

-- 	Find matches where the Dates are off 
SET @loopId = 1; 
WHILE @loopId <= 3 DO

	UPDATE 		bluecoat_bookings.BluecoatBookingFile book
	JOIN        (SELECT 	book.OrderNumber, book.QuoteSerialNumber, opp.oppUID, offer.offerUID, offer.predecessorUID
				 FROM 		bluecoat_bookings.BluecoatBookingFile book
				 JOIN		bluecoat_bookings.BluecoatBookingSummary bsum
					ON  	bsum.`OrderNumber` = book.`OrderNumber`
					AND 	bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber`
				 JOIN		bluecoat_bookings.OfferQuoteKey offer
					ON  	offer.offerName = book.`QuoteSerialNumber` 
					AND 	ROUND(bsum.Value,0) = ROUND(offer.offerAmount,0)
				 JOIN		bluecoat_bookings.OppQuoteKey opp
					ON  	opp.useQuoteUID = offer.QuoteUID
				 JOIN		bluecoat_bookings.SalesStageLoop stage
					ON 		opp.SalesStage = stage.stage AND stage.loopId = @loopId 
				 LEFT JOIN  bluecoat_bookings.BurnedOffers burnedOffers
					ON  	burnedOffers.offerUID = offer.offerUID 
				 LEFT JOIN  bluecoat_bookings.BurnedAssets burnedAssets
					ON  	burnedAssets.predecessorUID = offer.predecessorUID 
				 WHERE		book.isComplete = 0 
					AND 	book.offerUID IS NULL
					AND 	burnedOffers.offerUID IS NULL
					AND 	burnedAssets.predecessorUID IS NULL	
					AND 	DATE_ADD(offer.offerStart,INTERVAL 1 DAY) >= bsum.ServiceStart
					AND 	DATE_SUB(offer.offerStart,INTERVAL 1 DAY) <= bsum.ServiceEnd
				ORDER BY 	opp.oppUID, offer.offerUID, offer.predecessorUID) oppOffer ON oppOffer.OrderNumber = book.OrderNumber AND oppOffer.QuoteSerialNumber = book.QuoteSerialNumber
	SET			book.offerUID = oppOffer.offerUID,
				book.oppUID = oppOffer.oppUID,
				book.predecessorUID = oppOffer.predecessorUID,
				book.offerDesc = 'Date off'
	WHERE		book.isComplete = 0 
			AND book.offerUID IS NULL; -- 1 = 0 2= 292 3 = 19

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Date Off matched booking records for BluecoatBookingFile table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Date Off matched booking records for BluecoatBookingFile table for loopId = ', @loopId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;

		IF (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID) > 10 THEN 

			SET dupcount = (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID);
			SET result = CONCAT('DuplicateOfferUID exist in the BluecoatBookingFile table for Date loop = ', @loopId);

			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, 'DuplicateOfferUID exist in the BluecoatBookingFile table for Date loop';

			UPDATE bluecoat_bookings.BluecoatBookingFile book
			SET    book.offerDesc = NULL,
				   book.offerUID = NULL,
       		       book.oppUID = NULL,
				   book.predecessorUID = NULL
			WHERE	 book.isComplete = 0 
			AND EXISTS (SELECT dup.offerUID FROM bluecoat_bookings.DuplicateOfferUID dup WHERE dup.offerUID = book.offerUID)
			AND `OrderNumber` = (SELECT dup.`OrderNumber` FROM bluecoat_bookings.UniqueBookingRecords dup WHERE dup.offerUID = book.offerUID LIMIT 1);
 -- 0/0/0
			SET dupcount = ROW_COUNT();
			SET result = CONCAT('Date off Matched booking records removed duplicate offerId table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, result, dupcount;
     
		END IF;

     SET @loopId = @loopId + 1;

END WHILE;

SET step = 9; -- 9

-- 	Find matches where the Amount and Dates are off 
SET @loopId = 1; 
WHILE @loopId <= 3 DO

	UPDATE 		bluecoat_bookings.BluecoatBookingFile book
	JOIN        (SELECT 	book.OrderNumber, book.QuoteSerialNumber, opp.oppUID, offer.offerUID, offer.predecessorUID
				 FROM 		bluecoat_bookings.BluecoatBookingFile book
				 JOIN		bluecoat_bookings.BluecoatBookingSummary bsum
					ON  	bsum.`OrderNumber` = book.`OrderNumber`
					AND 	bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber`
				 JOIN		bluecoat_bookings.OfferQuoteKey offer
					ON  	offer.offerName = book.`QuoteSerialNumber` 
				 JOIN		bluecoat_bookings.OppQuoteKey opp
					ON  	opp.useQuoteUID = offer.QuoteUID
				 JOIN		bluecoat_bookings.SalesStageLoop stage
					ON 		opp.SalesStage = stage.stage AND stage.loopId = @loopId 
				 LEFT JOIN  bluecoat_bookings.BurnedOffers burnedOffers
					ON  	burnedOffers.offerUID = offer.offerUID 
				 LEFT JOIN  bluecoat_bookings.BurnedAssets burnedAssets
					ON  	burnedAssets.predecessorUID = offer.predecessorUID 
				 WHERE		book.isComplete = 0 
					AND 	book.offerUID IS NULL
					AND 	burnedOffers.offerUID IS NULL
					AND 	burnedAssets.predecessorUID IS NULL	
					AND 	DATE_ADD(offer.offerStart,INTERVAL 1 DAY) >= bsum.ServiceStart
					AND 	DATE_SUB(offer.offerStart,INTERVAL 1 DAY) <= bsum.ServiceEnd
				ORDER BY 	opp.oppUID, offer.offerUID, offer.predecessorUID) oppOffer ON oppOffer.OrderNumber = book.OrderNumber AND oppOffer.QuoteSerialNumber = book.QuoteSerialNumber  
	SET			book.offerUID = oppOffer.offerUID,
				book.oppUID = oppOffer.oppUID,
				book.predecessorUID = oppOffer.predecessorUID,
				book.offerDesc = 'Date/Amount off'
	WHERE		book.isComplete = 0 
			AND book.offerUID IS NULL; -- 1 = 33 2 = 906 3 = 30

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Amount/Date Off matched booking records for BluecoatBookingFile table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Amount/Date Off matched booking records for BluecoatBookingFile table for loopId = ', @loopId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;

		IF (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID) > 10 THEN 

			SET dupcount = (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID);
			SET result = CONCAT('DuplicateOfferUID exist in the BluecoatBookingFile table for Date/Amount off loop = ', @loopId);

			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, 'DuplicateOfferUID exist in the BluecoatBookingFile table for Date/Amount off loop';

			UPDATE bluecoat_bookings.BluecoatBookingFile book
			SET    book.offerDesc = NULL,
				   book.offerUID = NULL,
       		       book.oppUID = NULL,
				   book.predecessorUID = NULL
			WHERE	 book.isComplete = 0 
			AND EXISTS (SELECT dup.offerUID FROM bluecoat_bookings.DuplicateOfferUID dup WHERE dup.offerUID = book.offerUID)
			AND `OrderNumber` = (SELECT dup.`OrderNumber` FROM bluecoat_bookings.UniqueBookingRecords dup WHERE dup.offerUID = book.offerUID LIMIT 1);
 -- 0/2/4
			SET dupcount = ROW_COUNT();
			SET result = CONCAT('Amount off Matched booking records removed duplicate offerId table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, result, dupcount;
     
		END IF;

     SET @loopId = @loopId + 1;

END WHILE;


SET step = 10; -- 10

-- 	Find matches where the Dates off 60 days only closed sale 

	UPDATE 		bluecoat_bookings.BluecoatBookingFile book
	JOIN        (SELECT 	book.OrderNumber, book.QuoteSerialNumber, opp.oppUID, offer.offerUID, offer.predecessorUID
				 FROM 		bluecoat_bookings.BluecoatBookingFile book
				 JOIN		bluecoat_bookings.BluecoatBookingSummary bsum
					ON  	bsum.`OrderNumber` = book.`OrderNumber`
					AND 	bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber`
				 JOIN		bluecoat_bookings.OfferQuoteKey offer
					ON  	offer.offerName = book.`QuoteSerialNumber` 
					AND 	ROUND(bsum.Value,0) = ROUND(offer.offerAmount,0)
				 JOIN		bluecoat_bookings.OppQuoteKey opp
					ON  	opp.useQuoteUID = offer.QuoteUID
				 JOIN		bluecoat_bookings.SalesStageLoop stage
					ON 		opp.SalesStage = stage.stage AND stage.loopId = 1 
				 LEFT JOIN  bluecoat_bookings.BurnedOffers burnedOffers
					ON  	burnedOffers.offerUID = offer.offerUID 
				 LEFT JOIN  bluecoat_bookings.BurnedAssets burnedAssets
					ON  	burnedAssets.predecessorUID = offer.predecessorUID 
				 WHERE		book.isComplete = 0 
					AND 	book.offerUID IS NULL
					AND 	burnedOffers.offerUID IS NULL
					AND 	burnedAssets.predecessorUID IS NULL	
					AND 	DATE_ADD(offer.offerStart,INTERVAL 61 DAY) >= bsum.ServiceStart
					AND 	DATE_SUB(offer.offerStart,INTERVAL 61 DAY) <= bsum.ServiceEnd
				ORDER BY 	opp.oppUID, offer.offerUID, offer.predecessorUID) oppOffer ON oppOffer.OrderNumber = book.OrderNumber AND oppOffer.QuoteSerialNumber = book.QuoteSerialNumber
	SET			book.offerUID = oppOffer.offerUID,
				book.oppUID = oppOffer.oppUID,
				book.predecessorUID = oppOffer.predecessorUID,
				book.offerDesc = 'Date off'
	WHERE		book.isComplete = 0 
			AND book.offerUID IS NULL; -- 0

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Date Off 60 + closed matched booking records for BluecoatBookingFile table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Date Off 60 + closed matched booking records for BluecoatBookingFile table for loopId = ', @loopId, ' error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;

		IF (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID) > 10 THEN 

			SET dupcount = (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID);
			SET result = CONCAT('DuplicateOfferUID exist in the BluecoatBookingFile table for Date off 60 loop = ', @loopId);

			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, 'DuplicateOfferUID exist in the BluecoatBookingFile table for Date off 60 loop';

			UPDATE bluecoat_bookings.BluecoatBookingFile book
			SET    book.offerDesc = NULL,
				   book.offerUID = NULL,
       		       book.oppUID = NULL,
				   book.predecessorUID = NULL
			WHERE	 book.isComplete = 0 
			AND EXISTS (SELECT dup.offerUID FROM bluecoat_bookings.DuplicateOfferUID dup WHERE dup.offerUID = book.offerUID)
			AND `OrderNumber` = (SELECT dup.`OrderNumber` FROM bluecoat_bookings.UniqueBookingRecords dup WHERE dup.offerUID = book.offerUID LIMIT 1);
 
			SET dupcount = ROW_COUNT();
			SET result = CONCAT('Amount off Matched booking records removed duplicate offerId table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, result, dupcount;
     
		END IF;

SET step = 11; -- 11

-- 	Date/Amount off but in 30 day range - Open

	UPDATE 		bluecoat_bookings.BluecoatBookingFile book
	JOIN        (SELECT 	book.OrderNumber, book.QuoteSerialNumber, opp.oppUID, offer.offerUID, offer.predecessorUID
				 FROM 		bluecoat_bookings.BluecoatBookingFile book
				 JOIN		bluecoat_bookings.BluecoatBookingSummary bsum
					ON  	bsum.`OrderNumber` = book.`OrderNumber`
					AND 	bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber`
				 JOIN		bluecoat_bookings.OfferQuoteKey offer
					ON  	offer.offerName = book.`QuoteSerialNumber` 
				 JOIN		bluecoat_bookings.OppQuoteKey opp
					ON  	opp.useQuoteUID = offer.QuoteUID
				 JOIN		bluecoat_bookings.SalesStageLoop stage
					ON 		opp.SalesStage = stage.stage AND stage.loopId = 2 
				 LEFT JOIN  bluecoat_bookings.BurnedOffers burnedOffers
					ON  	burnedOffers.offerUID = offer.offerUID 
				 LEFT JOIN  bluecoat_bookings.BurnedAssets burnedAssets
					ON  	burnedAssets.predecessorUID = offer.predecessorUID 
				 WHERE		book.isComplete = 0 
					AND 	book.offerUID IS NULL
					AND 	burnedOffers.offerUID IS NULL
					AND 	burnedAssets.predecessorUID IS NULL	
					AND 	DATE_ADD(offer.offerStart,INTERVAL 31 DAY)  >= bsum.ServiceStart
					AND 	DATE_SUB(offer.offerStart,INTERVAL 31 DAY) <= bsum.ServiceEnd
				ORDER BY 	opp.oppUID, offer.offerUID, offer.predecessorUID) oppOffer ON oppOffer.OrderNumber = book.OrderNumber AND oppOffer.QuoteSerialNumber = book.QuoteSerialNumber   
	SET			book.offerUID = oppOffer.offerUID,
				book.oppUID = oppOffer.oppUID,
				book.predecessorUID = oppOffer.predecessorUID,
				book.offerDesc = 'Date off'
	WHERE		book.isComplete = 0 
			AND book.offerUID IS NULL; -- 8

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = 'Amount/Date Off 30 + open matched booking records for BluecoatBookingFile table';
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Amount/Date Off 30 + open matched booking records for BluecoatBookingFile table error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;

		IF (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID) > 10 THEN 

			SET dupcount = (SELECT COUNT(1) FROM bluecoat_bookings.DuplicateOfferUID);
			SET result = CONCAT('DuplicateOfferUID exist in the BluecoatBookingFile table for Date/Amount off 30 loop = ', @loopId);

			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, 'DuplicateOfferUID exist in the BluecoatBookingFile table for Date/Amount off 30 loop';

			UPDATE bluecoat_bookings.BluecoatBookingFile book
			SET    book.offerDesc = NULL,
				   book.offerUID = NULL,
       		       book.oppUID = NULL,
				   book.predecessorUID = NULL
			WHERE	 book.isComplete = 0 
			AND EXISTS (SELECT dup.offerUID FROM bluecoat_bookings.DuplicateOfferUID dup WHERE dup.offerUID = book.offerUID)
			AND `OrderNumber` = (SELECT dup.`OrderNumber` FROM bluecoat_bookings.UniqueBookingRecords dup WHERE dup.offerUID = book.offerUID LIMIT 1);
 
			SET dupcount = ROW_COUNT();
			SET result = CONCAT('Amount off Matched booking records removed duplicate offerId table for loopId = ', @loopId);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, dupcount);
			SELECT NOW(), 'matchBookings_sp', step, result, dupcount;
     
		END IF;

SET step = 12; -- 12

	UPDATE bluecoat_bookings.BluecoatBookingFile book
	JOIN   bluecoat_bookings.BluecoatBookingSummary bsum
	 ON 	bsum.`OrderNumber` = book.`OrderNumber`
	 AND 	bsum.`QuoteSerialNumber` = book.`QuoteSerialNumber`
	JOIN   bluecoat_bookings.OfferQuoteKey offer
	 ON 	offer.offeruid = book.offeruid
	 SET 	book.offerDESC = CONCAT('Date ',book.offerDESC)
	WHERE  book.isComplete = 0
	AND    book.offeruid IS NOT NULL
	AND    bsum.ServiceStart <> DATE(offer.offerstart)
	AND    book.offerDESC NOT LIKE '%Date%';

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = 'Updated rows to catch  non-matched start dates BluecoatBookingFile table';
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Failed to update rows to catch non-matched start dates BluecoatBookingFile error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;
/*  This is the end of the Matching Logic for the records  */
END IF;

/*
SET step = 12; -- 12

IF stepId <= step THEN  -- Restart point
-- 	mark those we want to close immediately because they are exact matches and not splitting

UPDATE       bluecoat_bookings.BluecoatBookingFile book
SET			 book.booked = 'complete', isComplete = 1, completeDate = NOW()
WHERE        book.oppUID IN (SELECT oppName FROM bluecoat_bookings.BluecoatCompleteNow ) ;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Update Closed Sale Exact matched booking records for BluecoatBookingFile table to Completed');
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;
			COMMIT;

		ELSE

			SET result = CONCAT('Update Closed Sale Exact matched booking records for BluecoatBookingFile table to Completed error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;
END IF;

-- 	mark those we need to further process via scrub
SET step = 13; -- 13

IF stepId <= step THEN  -- Restart point

UPDATE       bluecoat_bookings.BluecoatBookingFile book
SET			 book.booked = 'next'
WHERE        book.oppUID IN (SELECT oppName FROM bluecoat_bookings.BluecoatSetNextNow ) ;

		IF code = '00000' THEN

			SET count = ROW_COUNT();
			SET result = CONCAT('Update Open Sale Exact matched booking records for BluecoatBookingFile table to Next');
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, count);
			SELECT NOW(), 'matchBookings_sp', step, result, count;

			COMMIT;

		ELSE

			SET result = CONCAT('Update Open Sale Exact matched booking records for BluecoatBookingFile table to Next error = ', code, ', message = ', msg);
			INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
				(SELECT NOW(), 'matchBookings_sp', step, result, '');
			SELECT NOW(), 'matchBookings_sp', step, result, '';
			LEAVE matchBookings;

		END IF;
END IF;

*/

END;;
DELIMITER ;