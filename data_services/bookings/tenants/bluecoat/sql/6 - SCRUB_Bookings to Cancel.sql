
      
-- bookings to cancel (we might close them after splitting or resetting amounts later)

-- output to CSV
select distinct opp.Relationship_Booking_UID
from test.jjtmp_Bluecoat_Splits split
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_UID = split.opp_UID
where split.result = 'split'
  and split.salesStage in ('closedSale')
    union
select distinct opp.Relationship_Booking_UID
from test.jjtmp_Bluecoat_BookingFile book
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_UID = book.opp_UID  
where book.offer_uid is not null 
  and opp.salesStage in ('closedSale')
  and coalesce(book.booked,'') <> 'complete'
  and book.offer_DESC <> 'Exact'
    union
select distinct booking.DESTKEY
from bluecoat.APP_OPPORTUNITIES opps
left outer join bluecoat.RELATIONSHIPS booking
    on booking.SOURCETABLE = 'APP_OPPORTUNITIES'
    and booking.DESTTABLE = 'APP_BOOKINGS'
    and booking.SOURCEKEY = opps._ID
where opps.ISSUBORDINATE = 'undefined'
   and opps.FLOWS_SALESSTAGES_STATE_NAME in ('closedSale')
   and opps.DISPLAYNAME like '%Transitioned%'  ;  
   