DELIMITER ;;
CREATE DEFINER=`gpolitano`@`10.30.%.%` PROCEDURE `processedScrubsUpdate_sp`(

)
processedScrubs:
BEGIN

/****************************************************************************

** Name:    processedScrubsUpdate_sp
** Desc:    Processing of the csv files to 'scrub' the renew data is complete. Set the flag to
**				processed for the records that completed without error. This will allow us to 
**				review the data for the records that caused errors.
** Auth:    Grace Politano
** Date:    July 25, 2014
** Exec:    CALL bluecoat_bookings.processedScrubsUpdate_sp()
**************************
** Change History
**************************
** PR   Date	    Author   Description	
** --   --------   -------   ------------------------------------
** 

*****************************************************************************/
/*  Declare variables for status and error logging/reporting */

DECLARE step INT;
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

SET step = 1;
-- CancelBookings
UPDATE bluecoat_bookings.CancelBookings cb
JOIN   bluecoat_bookings.ScrubRenewData sd ON sd._id = cb.BookingId
	AND sd.dataScrub = 'CancelBookings'
    AND sd.isProcessed = 1
SET cb.isProcessed = 1
WHERE cb.isProcessed = 0;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, 'Updated the CancelBookings records for isProcessed', count);

ELSE

    SET result = CONCAT('Failed to update the records for CancelBookings, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '');
	SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '';

END IF;

SET step = 2;
-- CloseOpps
UPDATE bluecoat_bookings.CloseOpps co
JOIN   bluecoat_bookings.ScrubRenewData sd ON sd._id = co.OppId
	AND sd.dataScrub = 'CloseOpps'
    AND sd.isProcessed = 1
SET co.isProcessed = 1
WHERE co.isProcessed = 0;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, 'Updated the CloseOpps records for isProcessed', count);

ELSE

    SET result = CONCAT('Failed to update the records for CloseOpps, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '');
	SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '';

END IF;

SET step = 3;
-- RevertOppsToQuote
UPDATE bluecoat_bookings.RevertOppsToQuote ro
JOIN   bluecoat_bookings.ScrubRenewData sd ON sd._id = ro.OppId
	AND sd.dataScrub = 'RevertOppsToQuote'
    AND sd.isProcessed = 1
SET ro.isProcessed = 1
WHERE ro.isProcessed = 0;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, 'Updated the RevertOppsToQuote records for isProcessed', count);

ELSE

    SET result = CONCAT('Failed to update the records for RevertOppsToQuote, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '');
	SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '';

END IF;

SET step = 4;
-- SplitOpps
UPDATE bluecoat_bookings.SplitOpps so
JOIN   bluecoat_bookings.ScrubRenewData sd ON sd._id = so.OppId
	AND sd.dataScrub = 'SplitOpps'
    AND sd.isProcessed = 1
SET so.isProcessed = 1
WHERE so.isProcessed = 0;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, 'Updated the SplitOpps records for isProcessed', count);

ELSE

    SET result = CONCAT('Failed to update the records for SplitOpps, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '');
	SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '';

END IF;

SET step = 5;
-- TagAssets
UPDATE bluecoat_bookings.TagAssets ta
JOIN   bluecoat_bookings.ScrubRenewData sd ON sd._id = ta.AssetId
	AND sd.dataScrub = 'TagAssets'
    AND sd.isProcessed = 1
SET ta.isProcessed = 1
WHERE ta.isProcessed = 0;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, 'Updated the TagAssets records for isProcessed', count);

ELSE

    SET result = CONCAT('Failed to update the records for TagAssets, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '');
	SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '';

END IF;

SET step = 6;
-- UpdateOffers
UPDATE bluecoat_bookings.UpdateOffers uo
JOIN   bluecoat_bookings.ScrubRenewData sd ON sd._id = uo.OfferId
	AND sd.dataScrub = 'UpdateOffers'
    AND sd.isProcessed = 1
SET uo.isProcessed = 1
WHERE uo.isProcessed = 0;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, 'Updated the UpdateOffers records for isProcessed', count);

ELSE

    SET result = CONCAT('Failed to update the records for UpdateOffers, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '');
	SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '';

END IF;

SET step = 7;
-- UpdateOpps
UPDATE bluecoat_bookings.UpdateOpps uopp
JOIN   bluecoat_bookings.ScrubRenewData sd ON sd._id = uopp.OppId
	AND sd.dataScrub = 'UpdateOpps'
    AND sd.isProcessed = 1
SET uopp.isProcessed = 1
WHERE uopp.isProcessed = 0;

IF code = '00000' THEN

	SET count = ROW_COUNT();
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, 'Updated the UpdateOpps records for isProcessed', count);

ELSE

    SET result = CONCAT('Failed to update the records for UpdateOpps, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '');
	SELECT NOW(), 'processedScrubsUpdate_sp', step, result, '';

END IF;

END;;
DELIMITER ;