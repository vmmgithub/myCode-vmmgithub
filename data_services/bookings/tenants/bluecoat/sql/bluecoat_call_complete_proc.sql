select now(), ' Bluecoat marking records as processed' from DUAL;

use bluecoat_bookings;

call processedScrubsUpdate_sp();

select now(), ' Bluecoat marking records as processed' from DUAL;
