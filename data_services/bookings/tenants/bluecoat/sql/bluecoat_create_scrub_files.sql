/*
Call createScrubFiles_sp to create scrub files after the matching logic
has completed
*/

select now(), ' Bluecoat create scrub file procedures started' from DUAL;

use Xtenant_Config;


call createScrubFiles_sp('bluecoat','bluecoat_bookings','CancelBookings','/tmp/bluecoat/BlueCoat_CancelBookings','MAX');
call createScrubFiles_sp('bluecoat','bluecoat_bookings','RevertOppsToQuote','/tmp/bluecoat/BlueCoat_RevertOppsToQuote','MAX');
call createScrubFiles_sp('bluecoat','bluecoat_bookings','UpdateOffers','/tmp/bluecoat/BlueCoat_UpdateOffers','MAX');
call createScrubFiles_sp('bluecoat','bluecoat_bookings','UpdateOpps','/tmp/bluecoat/BlueCoat_UpdateOpps','MAX');
call createScrubFiles_sp('bluecoat','bluecoat_bookings','SplitOpps','/tmp/bluecoat/BlueCoat_SplitOpps','MAX');
call createScrubFiles_sp('bluecoat','bluecoat_bookings','CloseOpps','/tmp/bluecoat/BlueCoat_CloseOpps','MAX');
call createScrubFiles_sp('bluecoat','bluecoat_bookings','TagAssets','/tmp/bluecoat/BlueCoat_TagAssets','MAX');


select now(), ' Bluecoat create scrub file procedures completed' from DUAL;
