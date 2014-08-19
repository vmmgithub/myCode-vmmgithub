 
 ------------------------------------------------------
-- Opps to reset to quote delivered, essentially anything we need to change that is won/lost --
------------------------------------------------------  

-- closed/loss with amount or date scrubs needed
-- closed/loss with split needed
-- transitioned in sales stage other than transitioned
-- loss that need to close

-- output to CSV for scrub file
select distinct split.opp_uid
from test.jjtmp_Bluecoat_Splits split
where split.result = 'split'
  and split.salesStage in ('houseAccount','noService','closedSale','poReceived','customerCommitment')
    union
select distinct book.opp_uid
from test.jjtmp_Bluecoat_BookingFile book
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_UID = book.opp_UID  
where book.offer_uid is not null 
  and opp.salesStage in ('houseAccount','noService','closedSale')
  and book.offer_DESC <> 'Exact'
  and coalesce(book.booked,'') <> 'complete'
    union  
select distinct partner.opp_uid
from test.jjtmp_Bluecoat_BookingFile book
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_uid = book.opp_uid
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_uid
inner join test.bluecoat_offer_quote_key partner_offer
  on offer.relationship_predecessor_uid = partner_offer.relationship_predecessor_uid
inner join test.bluecoat_opp_partner_quote_key partner
  on partner.useQuote_UID = partner_offer.relationship_quote_uid 
  and partner.salesstage = opp.salesstage
  and partner.opp_name = opp.opp_name
where book.offer_uid is not null 
  and opp.salesStage in ('houseAccount','noService','closedSale')
  and book.offer_DESC <> 'Exact' 
  and coalesce(book.booked,'') <> 'complete' 
    union
select distinct book.opp_uid
from test.jjtmp_Bluecoat_BookingFile book
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_UID = book.opp_UID  
where book.offer_uid is not null 
  and opp.salesStage in ('houseAccount','noService')
    union 
select distinct partner.opp_uid
from test.jjtmp_Bluecoat_BookingFile book
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_uid = book.opp_uid
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_uid
inner join test.bluecoat_offer_quote_key partner_offer
  on offer.relationship_predecessor_uid = partner_offer.relationship_predecessor_uid
inner join test.bluecoat_opp_partner_quote_key partner
  on partner.useQuote_UID = partner_offer.relationship_quote_uid 
  and partner.salesstage = opp.salesstage
  and partner.opp_name = opp.opp_name
where book.offer_uid is not null 
  and opp.salesStage in ('houseAccount','noService') 
    union  
select distinct opps._ID
from bluecoat.APP_OPPORTUNITIES opps
where opps.ISSUBORDINATE = 'undefined'
   and opps.FLOWS_SALESSTAGES_STATE_NAME in ('closedSale')
   and opps.DISPLAYNAME like '%Transitioned%'  ;




