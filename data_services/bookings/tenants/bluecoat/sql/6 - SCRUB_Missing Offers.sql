-- offers missing - but have assets (need to TAG in order to gen manually)


-- output to CSV for scrub file
select distinct asset._ID
  ,bl.`Business Line`,book.`Quote Serial Number`,book.`booked date`
from test.jjtmp_Bluecoat_BookingFile book
left outer join test.jjtmp_bluecoat_reference_BusinessLine bl
  on book.`Item Description` = bl.ExistingServiceProduct
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`  
left outer join bluecoat.APP_ASSETS asset
  on asset.DISPLAYNAME = book.`Quote Serial Number`
  and asset.TYPE = 'app.asset/service'
left outer join bluecoat.RELATIONSHIPS rel
  on rel.DESTTABLE = 'APP_ASSETS'
  and rel.DESTKEY = asset._ID
  and rel.RELNAME = 'predecessor'
where book.offer_uid is null 
  and asset.DISPLAYNAME is not null and rel.relname is null
  order by 1;
  
 -- meaning the asset is there and an offer doesn't exist for it.















-------------------------------------------------------------------------------------

select distinct 
    book.`Order Number`,
    book.`Quote Serial Number`,
    bsum.ServiceStart,
    bsum.ServiceEnd,
    bsum.Value
from test.jjtmp_Bluecoat_BookingFile book
 left outer join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
 left outer join test.bluecoat_offer_quote_key offers
  on offers.offer_name = book.`Quote Serial Number`
  and not exists (select * from test.jjtmp_bluecoat_burned_offers burned -- these were already used
                  where burned.offer_uid = offers.offer_uid  ) 
where book.offer_uid is null 
  order by 1,2;  
      
 
select distinct 
    book.`Order Number`,
    book.`Quote Serial Number`
from test.jjtmp_Bluecoat_BookingFile book 
where book.offer_uid is null ;

-- validation

-- offers
select offers.offer_name,
  offers.offer_UID,
  offers.result_name,
  offers.offer_start,
  offers.offer_end,
  offers.offer_amount,
  opp.opp_name,
  opp.opp_UID,
  opp.SalesStage,
  opp.Relationship_Company
from test.bluecoat_offer_quote_key offers 
  inner join test.bluecoat_opp_quote_key opp
    on opp.useQuote_UID = offers.Relationship_Quote_UID
where offers.offer_name = 'KSA-300847PQNDB-24439';

-- bookings
select distinct 
  book.`Quote Serial Number`,
  book.`Order Number`,
  bsum.ServiceStart,
  bsum.ServiceEnd,
  bsum.Value,
  book.`End User`,
--  book.Customer , 
  book.offer_UID,
  book.offer_DESC,
  book.`Ordered Date`,
  book.`Booked Date`
from test.jjtmp_Bluecoat_BookingFile book 
 left outer join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
where book.`Quote Serial Number` = 'KSA-300847PQNDB-24439';
      
      
      select book.*
      from test.jjtmp_Bluecoat_BookingFile book 
where book.`Quote Serial Number` = '5107066103';
      
      
      
-- possible matches..      
select distinct 
    book.`Order Number`,
    book.`Quote Serial Number`,
    bsum.ServiceEnd,
    date(offer.offer_end) 'Renew End',
    bsum.value,
    offer.offer_amount,
    offer.offer_uid,
    opp.opp_uid,
    datediff(bsum.ServiceEnd,date(offer.offer_end)) as date_diff,
    round(bsum.value,0) - round(offer.offer_amount,0) as amount_diff
from test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage <> 'transitioned'  
where book.offer_uid is null 
order by book.`Order Number`,book.`Quote Serial Number` ;
  
  
  
  
  
  
  -----------------------------------------------------------------------------

-- bad matches based on old logic
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage <> 'transitioned'  
  and opp.SalesStage = 'closedSale' 
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.offer_DESC = 'Amount/Date off - SN match'
where book.offer_UID is null;
  
commit;

update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage <> 'transitioned'  
  and opp.SalesStage <> 'closedSale' 
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.offer_DESC = 'Amount/Date off - SN match'
where book.offer_UID is null;
  
commit;