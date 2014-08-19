-----------------------------
-- Offer Amount Scrub File --
-----------------------------
-- needs to be initial offer id --

-- output to CSV
select distinct book.offer_uid,
    round(coalesce(bsum.value,0),2) New_OfferAmount,
    'usd' New_Currency
from test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_UID 
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_UID = book.opp_UID  
where book.offer_uid is not null 
  and coalesce(book.booked,'') <> 'complete' 
  and book.offer_DESC like '%Amount%';  
  
  
---------------------------------
-- Offer End Date Scrub File --
---------------------------------
-- output to CSV
select distinct book.offer_uid,
    bsum.ServiceEnd New_EndDate
from test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_UID 
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_UID = book.opp_UID    
where book.offer_uid is not null 
  and coalesce(book.booked,'') <> 'complete' 
  and date(offer.offer_end) <> date(bsum.ServiceEnd)
  and book.offer_DESC like '%Date%'  ;  

-------------------------------
-- Offer Start Date Scrub File --
-------------------------------
-- output to CSV
select distinct book.offer_uid,
    bsum.ServiceStart New_StartDate
from test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_UID 
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_UID = book.opp_UID    
where book.offer_uid is not null 
  and coalesce(book.booked,'') <> 'complete' 
  and date(offer.offer_start) <> date(bsum.ServiceStart)
  and book.offer_DESC like '%Date%' ;  
  
  
-------------------------------
-- Offer EXCLUDED Scrub File --
-------------------------------
-- need both the base offer UID and the current in the scrub file as it is variable which one works.
-- including ALL VERSIONS (even those attached to partner opps) to get rid of the errors.
drop table if exists test.jjtmp_working_excluded;

commit;

create table test.jjtmp_working_excluded as (
select split.opp_uid,split.offer_UID as Book_Offers
from test.jjtmp_Bluecoat_Splits split
where split.result = 'book'   );

insert into test.jjtmp_working_excluded (opp_uid,Book_Offers)
select split.opp_uid,split.offer_UID_to_Split as Book_Offers
from test.jjtmp_Bluecoat_Splits split
where split.result = 'book'  ;

insert into test.jjtmp_working_excluded (opp_uid,Book_Offers)
select split.opp_uid,offer.offer_UID as Book_Offers
from test.jjtmp_Bluecoat_Splits split
inner join test.bluecoat_offer_quote_key offer
  on offer.Relationship_Predecessor_UID = split.Relationship_predecessor_UID
where split.result = 'book';

commit;

---
-- output to CSV
select distinct book.Book_Offers,
    'undefined' as is_excluded
from test.jjtmp_working_excluded book
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.Book_Offers 
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_UID = book.opp_UID    
where offer.isexcluded <> 'undefined'
  and coalesce(opp.salesstage,'') <> 'closedSale'
  union
select distinct book.Book_Offers,
    'undefined' as is_excluded
from test.jjtmp_working_excluded book
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.Book_Offers 
inner join test.bluecoat_opp_partner_quote_key opp
  on opp.useQuote_UID = offer.relationship_quote_uid  
where offer.isexcluded <> 'undefined'
  and coalesce(opp.salesstage,'') <> 'closedSale';    
  
  -- drop the temp table 
drop table if exists test.jjtmp_working_excluded;  
  
----------------------------------- 
-- Opportunity Amount scrub File --
-------------------------------
-- output to CSV
select distinct close.OppName ,
  close.New_OppAmount 	,
  'usd' New_Currency
from test.jjtmp_bluecoat_opp_value close
  inner join test.bluecoat_opp_quote_key opp
    on opp.opp_UID = close.OppName
where round(opp.Amount,0) <> round(close.New_OppAmount,0) 
  and opp.salesStage <> 'closedSale';  

  
-------------------------------------
-- Opportunity Expiration Scrub File --
-------------------------------
-- output to CSV
select distinct book.opp_uid,
    date_sub(min(bsum.ServiceStart),interval 1 day) New_ExpirationDate
from test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_UID 
inner join test.bluecoat_opp_quote_key opp
  on opp.opp_UID = book.opp_UID    
where book.offer_uid is not null 
  and coalesce(book.booked,'') <> 'complete' 
  and book.offer_DESC like '%Date%'
group by book.opp_uid  ;  


-------------------------------------
-- Opportunity Target Selling Period --
-------------------------------
-- create a temp table so we can compare selling periods
create table test.jjtmp_tempsell as (
select distinct book.opp_uid, 
  case
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    < '2013-08-01' then 'fy14q1'
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    between '2013-08-01' and '2013-10-31' then 'fy14q2'
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    between '2013-11-01' and '2014-01-31' then 'fy14q3'  
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    between '2014-02-01' and '2014-04-30' then 'fy14q4'    
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    between '2014-05-01' and '2014-07-31' then 'fy15q1'    
  end as targetSellingPeriod,opp.extensions_master_targetperiod_value_name
from test.jjtmp_Bluecoat_BookingFile book
inner join bluecoat.APP_OPPORTUNITIES opp
  on opp._ID = book.opp_UID    
where book.offer_uid is not null 
  and coalesce(book.booked,'') <> 'complete' 
and coalesce(opp.FLOWS_SALESSTAGES_STATE_NAME ,'') <> 'closedSale'
group by book.opp_uid);
commit;

insert into test.jjtmp_tempsell (opp_uid,targetSellingPeriod,extensions_master_targetperiod_value_name)
select distinct opp._ID, 
  case
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    < '2013-08-01' then 'fy14q1'
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    between '2013-08-01' and '2013-10-31' then 'fy14q2'
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    between '2013-11-01' and '2014-01-31' then 'fy14q3'  
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    between '2014-02-01' and '2014-04-30' then 'fy14q4'    
  when max(case when book.`Ordered Date` is null or book.`Ordered Date` = '' then cast(book.`Invoice Date` as date)
      else cast(book.`Ordered Date` as date) end) 
    between '2014-05-01' and '2014-07-31' then 'fy15q1'    
  end as targetSellingPeriod,opp.extensions_master_targetperiod_value_name
from test.jjtmp_Bluecoat_BookingFile book
inner join test.bluecoat_offer_quote_key offer
  on offer.Relationship_Predecessor_UID = book.predecessor_UID
inner join test.bluecoat_opp_partner_quote_key opp_partner
  on opp_partner.useQuote_UID = offer.relationship_quote_uid  
inner join bluecoat.APP_OPPORTUNITIES opp
  on opp._ID = opp_partner.opp_uid  
where book.offer_uid is not null 
and coalesce(opp.FLOWS_SALESSTAGES_STATE_NAME ,'') <> 'closedSale'
group by book.opp_uid  ;

commit;


-- output to CSV
select opp_uid,targetSellingPeriod 
from test.jjtmp_tempsell 
where targetSellingPeriod <> extensions_master_targetperiod_value_name;

-- drop the temp table
drop table if exists test.jjtmp_tempsell;





