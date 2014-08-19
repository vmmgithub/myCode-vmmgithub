/*
Call stored procedure that kicks off data matching logic.
getNewBookingRecords_sp
that store procedure kicks off
matchBookings_sp
that stored procedure kicks off
scrubRenewData_sp

after those 3 go, then ready to run createScrubFiles_sp
*/

select now(), ' Bluecoat stored procedures started' from DUAL;

use bluecoat_bookings;

call step1_getNewBookingRecords_sp(1);
