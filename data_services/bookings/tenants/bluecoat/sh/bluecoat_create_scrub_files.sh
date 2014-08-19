#!/bin/bash

# Call createScrubFiles_sp to create scrub files after the matching logic
# has completed


mysql -u $USERID -h$MYSQLHOST $SCHEMA << EOQUERY

select now(), ' ${TENANT} create scrub file procedures started' from DUAL;

use Xtenant_Config;


call createScrubFiles_sp('${TENANT}','${SCHEMA}','CancelBookings','${TMP_EX_HOME}/${CANCELBOOK}','MAX');
call createScrubFiles_sp('${TENANT}','${SCHEMA}','RevertOppsToQuote','${TMP_EX_HOME}/${REVERTOPPTOQUOTE}','MAX');
call createScrubFiles_sp('${TENANT}','${SCHEMA}','UpdateOffers','${TMP_EX_HOME}/${UPDATEOFF}','MAX');
call createScrubFiles_sp('${TENANT}','${SCHEMA}','UpdateOpps','${TMP_EX_HOME}/${UPDATEOPP}','MAX');
call createScrubFiles_sp('${TENANT}','${SCHEMA}','SplitOpps','${TMP_EX_HOME}/${SPLITOPP}','MAX');
call createScrubFiles_sp('${TENANT}','${SCHEMA}','CloseOpps','${TMP_EX_HOME}/${CLOSEOPP}','MAX');
call createScrubFiles_sp('${TENANT}','${SCHEMA}','TagAssets','${TMP_EX_HOME}/${TAGASSET}','MAX');


select now(), ' ${TENANT} create scrub file procedures completed' from DUAL;

EOQUERY
