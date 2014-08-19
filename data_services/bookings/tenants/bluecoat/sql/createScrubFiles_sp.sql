DELIMITER ;;
CREATE DEFINER=`gpolitano`@`10.30.%.%` PROCEDURE `createScrubFiles_sp`(
IN tenantname varchar(255), bookingschema varchar(255), datascrubname varchar(255), outputfilename varchar(500), ld char(3)
)
    SQL SECURITY INVOKER
createScrubFiles:

BEGIN

/****************************************************************************

** Name:    createScrubFiles_sp
** Desc:    
**			
**				
** Auth:    Trayton White
** Date:    July 23, 2014
** Exec:    CALL Xtenant_Config.createScrubFiles_sp('bluecoat','UpdateOffer')
**************************
** Change History
**************************
** PR   Date	    Author   Description	
** --   --------   -------   ------------------------------------
** 		2014-07-28   GMP	  Added logic to get All, Max, or Specific Load Data

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

IF ld = 'ALL' 
	THEN 
		SET @ldId = 0;  
		SET @ldSign = '>=';
	
END IF;

IF ld = 'MAX' 
	THEN 
		SET @ldId = -1;
		SET @ldSign = '=';

		SET @statement = CONCAT("SELECT MAX(loadId) INTO @ldId
								 FROM ", bookingschema, ".RenewScrubLoad
								 where status = 'DataReady'");
	
		PREPARE stmt FROM @statement;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;

END IF;

IF ld <> 'ALL' AND ld <> 'MAX'
	THEN 
		SET @ldId = ld;  
		SET @ldSign = '=';
	
END IF;
	
SET @statement = CONCAT("SELECT headerValues 
						 FROM Xtenant_Config.ScrubRenewDataHeader
						 WHERE dataScrub = '",datascrubname,"'
	
							UNION ALL

						 SELECT dataValues 
						 FROM ", bookingschema, ".ScrubRenewData
						 WHERE dataScrub = '",datascrubname,"'
						 AND isProcessed = 0
						 AND  loadId ",@ldSign,@ldId,"
						 


				INTO OUTFILE '", outputfilename, ".csv' CHARACTER SET utf8;");
	
PREPARE stmt FROM @statement;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

set @statement = concat("select shellscriptname
							FROM Xtenant_Config.ScrubRenewDataHeader
							WHERE datascrub = '", datascrubname, "'
							AND tenant = '", tenantname, "'
							into @jsscriptname;");
							
prepare stmt from @statement;
execute stmt;



call createJavascriptCommand(tenantname, datascrubname, @jsscriptname, outputfilename);	

deallocate prepare stmt;


END;;
DELIMITER ;