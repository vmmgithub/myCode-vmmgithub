DELIMITER ;;
CREATE DEFINER=`gpolitano`@`10.100.%.%` PROCEDURE `step1_getNewBookingRecords_sp`(
IN stepId int
)
getNewBookingRecords:

BEGIN

/****************************************************************************

** Name:    step1_getNewBookingRecords_sp
** Desc:    Import the 'new' data from the stage tables and insert into the bluecoat_bookings.BluecoatBookingFile.
**				Backup the existing bluecoat_bookings.BluecoatBookingFile in case of errors and manual intervention needed.
**				
** Auth:    Grace Politano
** Date:    July 23, 2014
** Exec:    CALL bluecoat_bookings.step1_getNewBookingRecords_sp(1)
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
DECLARE ldDate DATE;

/* Declare exception handler for failed steps */

DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
	BEGIN 
		GET DIAGNOSTICS CONDITION 1
			code = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
	END;

SET stepId = (SELECT IFNULL(stepId, 1));
SET ldDate = CURRENT_DATE;
SET step = 1;

	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, 'stored procedure started', '');
	SELECT NOW(), 'getNewBookingRecords_sp', step, 'stored procedure started', '';

IF stepId <= step THEN  -- Restart point

DROP TABLE IF EXISTS bluecoat_bookings.`BluecoatBookingBulkTemp`;
CREATE TABLE bluecoat_bookings.`BluecoatBookingBulkTemp` (
  `RowId` int PRIMARY KEY AUTO_INCREMENT,
  `OrderNumber` varchar(50) DEFAULT NULL,
  `OrderedDate` date DEFAULT NULL,
  `BookedDate` date DEFAULT NULL,
  `InvoiceNumber` varchar(50) DEFAULT NULL,
  `InvoiceDate` date DEFAULT NULL,
  `EntryStatus` varchar(50) DEFAULT NULL,
  `OrderType` varchar(50) DEFAULT NULL,
  `OrderSource` varchar(50) DEFAULT NULL,
  `PurchaseOrderNumber` varchar(50) DEFAULT NULL,
  `Customer` varchar(70) DEFAULT NULL,
  `CustomerNumber` varchar(50) DEFAULT NULL,
  `EndUser` varchar(85) DEFAULT NULL,
  `EndUserAccountNumber` varchar(50) DEFAULT NULL,
  `Reseller` varchar(82) DEFAULT NULL,
  `ResellerAccountNumber` varchar(50) DEFAULT NULL,
  `LineNumber` varchar(50) DEFAULT NULL,
  `LineType` varchar(52) DEFAULT NULL,
  `LineStatus` varchar(50) DEFAULT NULL,
  `PriceList` varchar(55) DEFAULT NULL,
  `Nsp` varchar(20) DEFAULT NULL,
  `Dan` varchar(20) DEFAULT NULL,
  `PartNumber` varchar(50) DEFAULT NULL,
  `ItemDescription` varchar(50) DEFAULT NULL,
  `ProductModel` varchar(50) DEFAULT NULL,
  `ServiceType` varchar(60) DEFAULT NULL,
  `ServiceStartDate` date DEFAULT NULL,
  `ServiceEndDate` date DEFAULT NULL,
  `SiebelQuoteNumber` varchar(52) DEFAULT NULL,
  `Quote` varchar(50) DEFAULT NULL,
  `QuoteLine` varchar(50) DEFAULT NULL,
  `QuoteSerialNumber` varchar(50) DEFAULT NULL,
  `OrderedQuantity` varchar(50) DEFAULT NULL,
  `SellingPrice` decimal(28,10) DEFAULT NULL,
  `Ext$Value` decimal(28,10) DEFAULT NULL,
  `LicenseCount` varchar(50) DEFAULT NULL,
  `SellingOrganization` varchar(57) DEFAULT NULL,
  `BillToState` varchar(50) DEFAULT NULL,
  `BillToCountry` varchar(50) DEFAULT NULL,
  `EndUserState` varchar(50) DEFAULT NULL,
  `EndUserCountry` varchar(50) DEFAULT NULL,
  `Region` varchar(50) DEFAULT NULL,
  `LoadDate` date DEFAULT NULL,
  KEY (`RowId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

  IF code = '00000' THEN

	SET count = ROW_COUNT();
	SET result = 'Temporary table dropped and recreated';
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, count);
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, count;

  ELSE

    SET result =CONCAT('Failed to drop and recreate the Temporary table, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, '');
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, '';
	LEAVE getNewBookingRecords;

  END IF;

END IF;

SET step = 2;

IF stepId <= step THEN  --

INSERT INTO bluecoat_bookings.`BluecoatBookingBulkTemp` (`OrderNumber`,`OrderedDate`,`BookedDate`,`InvoiceNumber`,`InvoiceDate`,`EntryStatus`,
														 `OrderType`,`OrderSource`,`PurchaseOrderNumber`,`Customer`,`CustomerNumber`,`EndUser`,
														 `EndUserAccountNumber`,`Reseller`,`ResellerAccountNumber`,`LineNumber`,`LineType`,
														 `LineStatus`,`PriceList`,`Nsp`,`Dan`,`PartNumber`,`ItemDescription`,`ProductModel`,
														 `ServiceType`,`ServiceStartDate`,`ServiceEndDate`,`SiebelQuoteNumber`,`Quote`,
														 `QuoteLine`,`QuoteSerialNumber`,`OrderedQuantity`,`SellingPrice`,`Ext$Value`,`LicenseCount`,
														 `SellingOrganization`,`BillToState`,`BillToCountry`,`EndUserState`,`EndUserCountry`,`Region`,
														 `LoadDate`)
SELECT 
	`OrderNumber`,
    STR_TO_DATE(`OrderedDate`,'%d-%M-%Y') OrderedDate,
    STR_TO_DATE(`BookedDate`,'%d-%M-%Y') BookedDate,
    `InvoiceNumber`,
    STR_TO_DATE(`InvoiceDate` ,'%d-%M-%Y') InvoiceDate,
    `EntryStatus`,
    `OrderType`,
    `OrderSource`,
    `PurchaseOrderNumber`,
    `Customer`,
    `CustomerNumber`,
    `EndUser`,
    `EndUserAccountNumber`,
    `Reseller`,
    `ResellerAccountNumber`,
    `LineNumber`,
    `LineType`,
    `LineStatus`,
    `PriceList`,
    `Nsp`,
    `Dan`,
    `PartNumber`,
    `ItemDescription`,
    `ProductModel`,
    `ServiceType`,
    STR_TO_DATE(`ServiceStartDate` ,'%d-%M-%Y') ServiceStartDate,
    STR_TO_DATE(`ServiceEndDate` ,'%d-%M-%Y') ServiceEndDate,
    `SiebelQuoteNumber`,
    `Quote`,
    `QuoteLine`,
    CASE 
    	WHEN `QuoteSerialNumber` IS NULL THEN CONCAT(ItemDescription, ' SO # ', OrderNumber) 
    	WHEN `QuoteSerialNumber` = '' THEN CONCAT(ItemDescription, ' SO # ', OrderNumber) 
		ELSE QuoteSerialNumber
    END AS   `QuoteSerialNumber`,
    `OrderedQuantity`,
    REPLACE(`SellingPrice`, ',', '') SellingPrice,
    REPLACE(`Ext$Value`, ',', '') Ext$Value,
    `LicenseCount`,
    `SellingOrganization`,
    `BillToState`,
    `BillToCountry`,
    `EndUserState`,
    `EndUserCountry`,
    `Region`,
	 ldDate AS LoadDate
FROM `bluecoat_bookings`.`BluecoatBookingBulk`
UNION
SELECT
    `InvoiceNumber` AS OrderNumber,
    STR_TO_DATE(`InvoiceDate` ,'%d-%M-%Y')   AS   OrderedDate,
    STR_TO_DATE(`InvoiceDate` ,'%d-%M-%Y')   AS   BookedDate,
    NULL  AS  InvoiceNumber,
   STR_TO_DATE(`InvoiceDate` ,'%d-%M-%Y') ,
    NULL  AS   EntryStatus,
    NULL  AS   OrderType,
    NULL  AS   OrderSource,
   `CustomerPoNumber`   AS  PurchaseOrderNumber,
   `BillToCustomerName` AS  Customer,
    NULL  AS   CustomerNumber,
   `EndCustomer`  AS   EndUser,
    NULL  AS   EndUserAccountNumber,
   `ResellerName` AS   Reseller,
    NULL AS  ResellerAccountNumber,
    NULL AS  LineNumber,
    NULL AS  LineType,
    NULL AS  LineStatus,
    NULL AS  PriceList,
    NULL AS  Nsp,
    NULL AS  Dan,
    NULL AS  PartNumber,
   `ServiceName`  AS  ItemDescription,
    NULL AS  ProductModel,
    NULL AS  ServiceType,
   STR_TO_DATE(`StartDate` ,'%d-%M-%Y')  AS    ServiceStartDate,
   STR_TO_DATE(`EndDate` ,'%d-%M-%Y')    AS    ServiceEndDate,
    NULL  AS SiebelQuoteNumber,
    NULL  AS Quote,
    NULL  AS QuoteLine,
   `SerialNumber` AS     QuoteSerialNumber,
   `QuantityInvoiced`  AS  OrderedQuantity,
   REPLACE(`UnitSellingPrice`, ',', '')   AS  SellingPrice,
   REPLACE(`ExtendedAmount`, ',', '')    AS Ext$Value,
    NULL  AS LicenseCount,
    NULL  AS SellingOrganization,
    NULL  AS BillToState,
    NULL  AS BillToCountry,
    NULL  AS EndUserState,
   `EndUserCountry`  AS  EndUserCountry,
   `Region`    Region,
	ldDate AS LoadDate
FROM `bluecoat_bookings`.`BluecoatBookingBulkOSC`
ORDER BY OrderNumber, InvoiceDate, LineNumber;

  IF code = '00000' THEN

	SET count = ROW_COUNT();
	SET result = 'Inserted the new data into the temporary table';
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, count);
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, count;

  ELSE

    SET result = CONCAT('Failed to insert the new data into the temporary table, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, '');
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, '';
	LEAVE getNewBookingRecords;

  END IF;

END IF;

SET step = 3;

IF stepId <= step THEN  --

UPDATE bluecoat_bookings.`BluecoatBookingBulkTemp` b
JOIN  bluecoat_bookings.`BluecoatBookingBulkTemp` bb ON b.OrderNumber = bb.OrderNumber
		AND bb.RowId = (b.RowId - 1)
SET   b.ServiceStartDate = bb.ServiceStartDate, 
	  b.ServiceEndDate = bb.ServiceEndDate
WHERE b.ServiceStartDate IS NULL 
OR    b.ServiceStartDate = '0000-00-00'
OR    b.ServiceEndDate IS NULL
OR    b.ServiceEndDate = '0000-00-00';

  IF code = '00000' THEN

	SET count = ROW_COUNT();
	SET result = 'Update the missing ServiceStateDate & ServiceEndDate values';
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, count);
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, count;

  ELSE

    SET result = CONCAT('Failed to update the missing ServiceStateDate & ServiceEndDate values, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, '');
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, '';
	LEAVE getNewBookingRecords;

  END IF;

  IF (SELECT COUNT(1) FROM bluecoat_bookings.`BluecoatBookingBulkTemp` WHERE ServiceStartDate IS NULL OR ServiceStartDate = '0000-00-00' 
		OR ServiceEndDate IS NULL OR ServiceEndDate = '0000-00-00') > 0 THEN

  SET step = 4;

	UPDATE bluecoat_bookings.`BluecoatBookingBulkTemp` b
	JOIN  bluecoat_bookings.`BluecoatBookingBulkTemp` bb ON b.OrderNumber = bb.OrderNumber
			AND bb.RowId = (b.RowId - 1)
	SET   b.ServiceStartDate = CASE WHEN bb.ServiceStartDate =  '0000-00-00' OR bb.ServiceStartDate IS NULL THEN DATE_ADD(b.InvoiceDate, INTERVAL 1 DAY) ELSE bb.ServiceStartDate END, 
		b.ServiceEndDate =  CASE WHEN bb.ServiceEndDate =  '0000-00-00' OR bb.ServiceEndDate IS NULL THEN DATE_ADD(b.InvoiceDate, INTERVAL 1 YEAR) ELSE bb.ServiceEndDate END
	WHERE b.ServiceStartDate IS NULL 
	OR    b.ServiceStartDate = '0000-00-00'
	OR    b.ServiceEndDate IS NULL
	OR    b.ServiceEndDate = '0000-00-00';

	IF code = '00000' THEN

		SET count = ROW_COUNT();
		SET result = 'Update the missing ServiceStateDate & ServiceEndDate values';
		INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
			(SELECT NOW(), 'getNewBookingRecords_sp', step, result, count);
		SELECT NOW(), 'getNewBookingRecords_sp', step, result, count;

	ELSE

		SET result = CONCAT('Failed to update the missing ServiceStateDate & ServiceEndDate values, error = ', code, ', message = ', msg);
		INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
			(SELECT NOW(), 'getNewBookingRecords_sp', step, result, '');
		SELECT NOW(), 'getNewBookingRecords_sp', step, result, '';
		LEAVE getNewBookingRecords;

	END IF;

  END IF;

  SET step = 5;

	UPDATE bluecoat_bookings.`BluecoatBookingBulkTemp` b
	SET   b.ServiceStartDate = DATE_ADD(b.InvoiceDate, INTERVAL 1 DAY) , 
		b.ServiceEndDate =  DATE_ADD(b.InvoiceDate, INTERVAL 1 YEAR) 
	WHERE b.ServiceStartDate IS NULL 
	OR    b.ServiceStartDate = '0000-00-00'
	OR    b.ServiceEndDate IS NULL
	OR    b.ServiceEndDate = '0000-00-00';

	IF code = '00000' THEN

		SET count = ROW_COUNT();
		SET result = 'Update the missing ServiceStateDate & ServiceEndDate values';
		INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
			(SELECT NOW(), 'getNewBookingRecords_sp', step, result, count);
		SELECT NOW(), 'getNewBookingRecords_sp', step, result, count;

	ELSE

		SET result = CONCAT('Failed to update the missing ServiceStateDate & ServiceEndDate values, error = ', code, ', message = ', msg);
		INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
			(SELECT NOW(), 'getNewBookingRecords_sp', step, result, '');
		SELECT NOW(), 'getNewBookingRecords_sp', step, result, '';
		LEAVE getNewBookingRecords;

	END IF;

END IF;

SET step = 6;

IF stepId <= step THEN  --

SELECT DISTINCT book.`QuoteSerialNumber` 
FROM		bluecoat_bookings.BluecoatBookingBulk book
LEFT JOIN	bluecoat.APP_ASSETS asset
		ON asset.DISPLAYNAME = book.`QuoteSerialNumber`
		AND asset.TYPE = 'app.asset/service'
WHERE	asset._ID IS NULL
		AND LENGTH(book.`QuoteSerialNumber`) < 10
		AND LEFT(book.`QuoteSerialNumber`,1) IN ('1','2','3','4','5','6','7','8','9')
UNION
SELECT DISTINCT book.`SerialNumber` 
FROM		bluecoat_bookings.BluecoatBookingBulkOSC book
LEFT JOIN	bluecoat.APP_ASSETS asset
		ON asset.DISPLAYNAME = book.`SerialNumber`
		AND asset.TYPE = 'app.asset/service'
WHERE	 asset._ID IS NULL
		 AND LENGTH(book.`SerialNumber`) < 10
		AND LEFT(book.`SerialNumber`,1) IN ('1','2','3','4','5','6','7','8','9');

  IF code = '00000' THEN

	SET count = ROW_COUNT();
	SET result = CASE WHEN count > 0 THEN 'ALERT QuoteSerialNumber missing first zero character in value'
						ELSE 'Checked for QuoteSerialNumber missing first zero character in value no matching records' END;
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, count);
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, count;

  ELSE

    SET result = CONCAT('Failed to Checked for QuoteSerialNumber missing first zero character in value, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, '');
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, '';
	LEAVE getNewBookingRecords;

  END IF;

END IF;

SET step = 7;

IF stepId <= step THEN  --

INSERT INTO `bluecoat_bookings`.`BluecoatBookingFile`
(`OrderNumber`,
`OrderedDate`,
`BookedDate`,
`InvoiceNumber`,
`InvoiceDate`,
`EntryStatus`,
`OrderType`,
`OrderSource`,
`PurchaseOrderNumber`,
`Customer`,
`CustomerNumber`,
`EndUser`,
`EndUserAccountNumber`,
`Reseller`,
`ResellerAccountNumber`,
`LineNumber`,
`LineType`,
`LineStatus`,
`PriceList`,
`Nsp`,
`Dan`,
`PartNumber`,
`ItemDescription`,
`ProductModel`,
`ServiceType`,
`ServiceStartDate`,
`ServiceEndDate`,
`SiebelQuoteNumber`,
`Quote`,
`QuoteLine`,
`QuoteSerialNumber`,
`OrderedQuantity`,
`SellingPrice`,
`Ext$Value`,
`LicenseCount`,
`SellingOrganization`,
`BillToState`,
`BillToCountry`,
`EndUserState`,
`EndUserCountry`,
`Region`,
`loadDate`,
`isComplete`)
SELECT `OrderNumber`,
    `OrderedDate`,
    `BookedDate`,
    `InvoiceNumber`,
    `InvoiceDate`,
    `EntryStatus`,
    `OrderType`,
    `OrderSource`,
    `PurchaseOrderNumber`,
    `Customer`,
    `CustomerNumber`,
    `EndUser`,
    `EndUserAccountNumber`,
    `Reseller`,
    `ResellerAccountNumber`,
    `LineNumber`,
    `LineType`,
    `LineStatus`,
    `PriceList`,
    `Nsp`,
    `Dan`,
    `PartNumber`,
    `ItemDescription`,
    `ProductModel`,
    `ServiceType`,
    `ServiceStartDate`,
    `ServiceEndDate`,
    `SiebelQuoteNumber`,
    `Quote`,
    `QuoteLine`,
    `QuoteSerialNumber`,
    `OrderedQuantity`,
    `SellingPrice`,
    `Ext$Value`,
    `LicenseCount`,
    `SellingOrganization`,
    `BillToState`,
    `BillToCountry`,
    `EndUserState`,
    `EndUserCountry`,
    `Region`,
    `loadDate`,
    0
FROM `bluecoat_bookings`.`BluecoatBookingBulkTemp`;

  IF code = '00000' THEN

	SET count = ROW_COUNT();
	SET result = 'Inserted the data into the BluecoatBookingFile table';
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, count);
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, count;
	CALL bluecoat_bookings.step2_matchBookings_sp(1);

  ELSE

    SET result = CONCAT('Failed to insert the data into the BluecoatBookingFile table, error = ', code, ', message = ', msg);
	INSERT INTO bluecoat_bookings.BookingsLogFile (logTime, process, stepId, message, rowCount)
		(SELECT NOW(), 'getNewBookingRecords_sp', step, result, '');
	SELECT NOW(), 'getNewBookingRecords_sp', step, result, '';
	LEAVE getNewBookingRecords;

  END IF;

END IF;

END;;
DELIMITER ;