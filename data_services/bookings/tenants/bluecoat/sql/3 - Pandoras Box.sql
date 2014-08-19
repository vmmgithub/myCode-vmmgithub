
set tmp_table_size=6000000000;
set max_heap_table_size=6000000000;

-- in case we need to regenerate indexes

-- alter table test.jjtmp_Bluecoat_BookingFile add index idx_jjtmp_Bluecoat_BookingFileopp_UID (opp_UID);
-- alter table test.jjtmp_Bluecoat_BookingFile add index idx_jjtmp_Bluecoat_BookingFileoffer_UID (offer_UID);

-- create backup
drop table if exists test.jjtmp_Bluecoat_BookingFile_backup;
create table test.jjtmp_Bluecoat_BookingFile_backup as (select distinct * from test.jjtmp_Bluecoat_BookingFile);
commit;

delete from test.jjtmp_Bluecoat_BookingFile
where `Quote Serial Number` is null;

commit;

-- reset the records we should have booked, just in case they didn't
update test.jjtmp_Bluecoat_BookingFile book
set book.booked=null
where book.booked = 'next';

commit;

-- create an order / SN summary for matching and updating --

drop table if exists test.jjtmp_bluecoat_SUM;

create table test.jjtmp_bluecoat_SUM as (
select book.`Order Number`,
  book.`Quote Serial Number`,
  sum(coalesce(book.`Selling Price`,0)) as Value,
  MIN(date(str_to_date(coalesce(left(book.`Service start date`,10),'2020-12-31'),'%Y-%m-%d'))) as ServiceStart,
  MAX(date(str_to_date(coalesce(left(book.`Service end date`,10),'2000-12-31'),'%Y-%m-%d'))) as ServiceEnd,
  min(book.`Item Description`) as product,
  book.`Part Number`
from test.jjtmp_Bluecoat_BookingFile book
group by book.`Order Number`,book.`Quote Serial Number`);

commit;


update test.jjtmp_bluecoat_SUM set ServiceEnd = null where date(ServiceEnd) = '2000-12-31';
update test.jjtmp_bluecoat_SUM set ServiceStart = null where date(ServiceStart) = '2020-12-31';

commit;

-- fill in the service dates where they are null 
update test.jjtmp_bluecoat_SUM bsum
inner join test.jjtmp_Bluecoat_BookingFile book
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
set bsum.ServiceStart = book.`Invoice Date`
where bsum.ServiceStart is null;

commit;

-- fill in the service end dates where they are null
update test.jjtmp_bluecoat_SUM bsum
inner join test.jjtmp_Bluecoat_BookingFile book
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
set bsum.ServiceEnd = date_sub(date_add(bsum.servicestart,interval 1 year),interval 1 day)
where bsum.ServiceEnd is null;

commit;


-- create our indexes for faster processing --
alter table test.jjtmp_bluecoat_SUM add index idx_jjtmp_bluecoat_SUM_order (`Order Number`);

commit;

-- ------------------------- --
-- create a list of opps we've already used to avoid --

drop table if exists test.jjtmp_bluecoat_burned_opps ;

create table test.jjtmp_bluecoat_burned_opps as (
  select distinct book.opp_UID
  from test.jjtmp_Bluecoat_BookingFile book
  where coalesce(book.Booked,'') = 'complete' );
  
commit;

alter table test.jjtmp_bluecoat_burned_opps add index jjtmp_bluecoat_burned_opps (opp_UID);


-- check if we need to update our working offers to make sure we have the most recent offer_uid and opp_uid in case of splits/updates/consolidations
update test.jjtmp_Bluecoat_BookingFile book
left outer join test.bluecoat_offer_quote_key orig_offer
  on orig_offer.offer_uid = book.offer_uid
left outer join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = orig_offer.Relationship_Quote_UID
set book.offer_DESC = 'moved'
where book.offer_UID is not null
  and opp.opp_uid is null
  and coalesce(book.Booked,'') <> 'complete';

commit;

-- get the new offer UID, make a few passes to make sure we get the Best, Better, Good match
update test.jjtmp_Bluecoat_BookingFile book
inner join test.bluecoat_offer_quote_key orig_offer
  on orig_offer.offer_uid = book.offer_uid
inner join test.bluecoat_offer_quote_key new_offer  
  on new_offer.relationship_predecessor_uid = orig_offer.relationship_predecessor_uid
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = new_offer.Relationship_Quote_UID
left outer join test.jjtmp_bluecoat_burned_opps burned_opps
  on burned_opps.opp_uid = opp.opp_uid
set book.opp_uid = opp.opp_uid,
  book.offer_uid = new_offer.offer_uid,
  book.offer_DESC = null
where book.offer_UID is not null
  and new_offer.offer_uid is not null
  and burned_opps.opp_uid is null
  and book.offer_DESC = 'moved'
  and new_offer.offer_uid <> book.offer_uid
  and opp.salesstage = 'closedSale'
  and coalesce(book.Booked,'') <> 'complete';

commit;

-- get the new offer UID, make a few passes to make sure we get the Best, Better, Good match
update test.jjtmp_Bluecoat_BookingFile book
inner join test.bluecoat_offer_quote_key orig_offer
  on orig_offer.offer_uid = book.offer_uid
inner join test.bluecoat_offer_quote_key new_offer  
  on new_offer.relationship_predecessor_uid = orig_offer.relationship_predecessor_uid
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = new_offer.Relationship_Quote_UID
left outer join test.jjtmp_bluecoat_burned_opps burned_opps
  on burned_opps.opp_uid = opp.opp_uid
set book.opp_uid = opp.opp_uid,
  book.offer_uid = new_offer.offer_uid,
  book.offer_DESC = null
where book.offer_UID is not null
  and new_offer.offer_uid is not null
  and burned_opps.opp_uid is null
  and book.offer_DESC = 'moved'
  and new_offer.offer_uid <> book.offer_uid
  and opp.SalesStage in ('closedSale','poReceived','customerCommitment','quoteCompleted','quoteRequested','quoteDelivered','contacted','notContacted') 
  and coalesce(book.Booked,'') <> 'complete';

commit;

-- get the new offer UID, make a few passes to make sure we get the Best, Better, Good match
update test.jjtmp_Bluecoat_BookingFile book
inner join test.bluecoat_offer_quote_key orig_offer
  on orig_offer.offer_uid = book.offer_uid
inner join test.bluecoat_offer_quote_key new_offer  
  on new_offer.relationship_predecessor_uid = orig_offer.relationship_predecessor_uid
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = new_offer.Relationship_Quote_UID
left outer join test.jjtmp_bluecoat_burned_opps burned_opps
  on burned_opps.opp_uid = opp.opp_uid
set book.opp_uid = opp.opp_uid,
  book.offer_uid = new_offer.offer_uid,
  book.offer_DESC = null
where book.offer_UID is not null
  and new_offer.offer_uid is not null
  and burned_opps.opp_uid is null
  and book.offer_DESC = 'moved'
  and new_offer.offer_uid <> book.offer_uid
  and coalesce(book.Booked,'') <> 'complete';

commit;



-- ------------------------- --
-- create a list of offers and assets we've already used to avoid (one per order/SN combination)--

drop table if exists test.jjtmp_bluecoat_burned_offers ;

create table test.jjtmp_bluecoat_burned_offers as (
  select distinct book.offer_UID
  from test.jjtmp_Bluecoat_BookingFile book);
  
commit;

alter table test.jjtmp_bluecoat_burned_offers add index jjtmp_bluecoat_burned_offers (offer_UID);

drop table if exists test.jjtmp_bluecoat_burned_assets ;

create table test.jjtmp_bluecoat_burned_assets as (
  select distinct book.predecessor_UID
  from test.jjtmp_Bluecoat_BookingFile book);
  
commit;

alter table test.jjtmp_bluecoat_burned_assets add index jjtmp_bluecoat_burned_assets (predecessor_UID);


-- clear the status of any offers that were scrubbed and not complete --
update test.jjtmp_Bluecoat_BookingFile book
set book.offer_DESC = null
where book.offer_UID is not null
  and coalesce(book.Booked,'') <> 'complete';

commit;


-- reupdate the status of these previously matched offers 
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_uid
  and bsum.ServiceEnd = date(offer.offer_end) 
  and round(bsum.value,0) = round(offer.offer_amount,0)
set book.offer_DESC = 'Exact'
where book.offer_UID is not null
  and coalesce(book.Booked,'') <> 'complete';

commit;

update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_uid
  and round(bsum.value,0) = round(offer.offer_amount,0)
set book.offer_DESC = 'Date off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is not null
  and bsum.ServiceEnd <> date(offer.offer_end);

commit;

update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_uid
  and bsum.ServiceEnd = date(offer.offer_end)
set book.offer_DESC = 'Amount off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is not null
  and round(bsum.value,0) <> round(offer.offer_amount,0)  ;

commit;

update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_uid
set book.offer_DESC = 'Amount/Date off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is not null
  and book.offer_DESC is null ;

commit;

--------------------------
-- BEGIN Matching Passes

-- 1.	EXACT – Closed Sale
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and bsum.ServiceEnd = date(offer.offer_end) 
  and round(bsum.value,0) = round(offer.offer_amount,0)
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage = 'closedSale'
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid 
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid   
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Exact'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;


  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  


  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  -- END Prep for next pass
  -- ----------------------------- --


-- -- -- -- -- -- -- -- --
-- 2.	 EXACT - Open

update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and bsum.ServiceEnd = date(offer.offer_end) 
  and round(bsum.value,0) = round(offer.offer_amount,0)
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage in ('closedSale','poReceived','customerCommitment','quoteCompleted','quoteRequested','quoteDelivered','contacted','notContacted') 
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid   
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Exact'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;


  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  -- END Prep for next pass
  -- ----------------------------- --

-- -- -- -- -- -- -- -- --
-- 3.	EXACT - Any
 update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and bsum.ServiceEnd = date(offer.offer_end) 
  and round(bsum.value,0) = round(offer.offer_amount,0)
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid  
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Exact'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;



  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --

-- -- -- -- -- -- -- -- --
-- 4.	Amount off – Closed Sale
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and bsum.ServiceEnd = date(offer.offer_end) 
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid  
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid    
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage = 'closedSale' 
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Amount off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;


 
  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  -- END Prep for next pass
  -- ----------------------------- --

  
-- -- -- -- -- -- -- -- --
-- 5.	Amount off - Open
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and bsum.ServiceEnd = date(offer.offer_end) 
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid  
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid   
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage in ('closedSale','poReceived','customerCommitment','quoteCompleted','quoteRequested','quoteDelivered','contacted','notContacted') 
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Amount off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;


  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --
  
-- -- -- -- -- -- -- -- --
-- 6.	Amount off - Any
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and bsum.ServiceEnd = date(offer.offer_end) 
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid 
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid    
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Amount off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;


  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --


-- -- -- -- -- -- -- -- --
-- 7.	Dates off but in range – Closed Sale
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and round(bsum.value,0) = round(offer.offer_amount,0)
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid  
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid    
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage = 'closedSale' 
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Date off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and date_add(offer.offer_start,interval 1 day) >= bsum.ServiceStart
  and date_sub(offer.offer_start,interval 1 day) <= bsum.ServiceEnd
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;



  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --

-- -- -- -- -- -- -- -- --
-- 8.	Dates off but in range - Open
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and round(bsum.value,0) = round(offer.offer_amount,0)
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid  
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid     
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
   and opp.SalesStage in ('closedSale','poReceived','customerCommitment','quoteCompleted','quoteRequested','quoteDelivered','contacted','notContacted') 
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Date off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and date_add(offer.offer_start,interval 1 day) >= bsum.ServiceStart
  and date_sub(offer.offer_start,interval 1 day) <= bsum.ServiceEnd
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;



  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --

-- -- -- -- -- -- -- -- --
-- 9.	Dates off but in range - Any
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and round(bsum.value,0) = round(offer.offer_amount,0)
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid  
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid   
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Date off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and date_add(offer.offer_start,interval 1 day) >= bsum.ServiceStart
  and date_sub(offer.offer_start,interval 1 day) <= bsum.ServiceEnd
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;



  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --

-- -- -- -- -- -- -- -- --
-- 10.	Date/Amount off – Closed Sale
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid 
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid     
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage = 'closedSale'
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Date/Amount off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and date_add(offer.offer_start,interval 1 day) >= bsum.ServiceStart
  and date_sub(offer.offer_start,interval 1 day) <= bsum.ServiceEnd
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;



  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
   
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
 
  -- END Prep for next pass
  -- ----------------------------- --

-- -- -- -- -- -- -- -- --
-- 11.	Date/Amount off - Open
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid 
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid     
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage in ('closedSale','poReceived','customerCommitment','quoteCompleted','quoteRequested','quoteDelivered','contacted','notContacted') 
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Date/Amount off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and date_add(offer.offer_start,interval 1 day) >= bsum.ServiceStart
  and date_sub(offer.offer_start,interval 1 day) <= bsum.ServiceEnd
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;


  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --

-- -- -- -- -- -- -- -- --
-- 12.	Date/Amount off - Any
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid  
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid    
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Date/Amount off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and date_add(offer.offer_start,interval 1 day) >= bsum.ServiceStart
  and date_sub(offer.offer_start,interval 1 day) <= bsum.ServiceEnd
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;


  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --


-- -- -- -- -- -- -- -- --
-- 13.	Dates off but in 60 day range – Closed Sale
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
  and round(bsum.value,0) = round(offer.offer_amount,0)
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid 
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid    
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage = 'closedSale'
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Date off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and offer.offer_start >= date_sub(bsum.ServiceStart, interval 61 day)
  and offer.offer_start <= date_add(bsum.ServiceEnd, interval 61 day)
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;



  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --

-- -- -- -- -- -- -- -- --
-- 14.	Date/Amount off but in 30 day range - Open
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_name = book.`Quote Serial Number`
left outer join test.jjtmp_bluecoat_burned_offers burned
  on burned.offer_uid = offer.offer_uid 
left outer join test.jjtmp_bluecoat_burned_assets burned_assets
  on burned_assets.predecessor_uid = offer.relationship_predecessor_uid    
inner join test.bluecoat_opp_quote_key opp
  on opp.useQuote_UID = offer.Relationship_Quote_UID
  and opp.SalesStage in ('closedSale','poReceived','customerCommitment','quoteCompleted','quoteRequested','quoteDelivered','contacted','notContacted') 
set book.offer_UID = offer.offer_uid,
  book.opp_UID = opp.opp_UID,
  book.predecessor_UID = offer.relationship_predecessor_uid,
  book.offer_DESC = 'Date/Amount off'
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is null
  and offer.offer_start >= date_sub(bsum.ServiceStart, interval 31 day)
  and offer.offer_start <= date_add(bsum.ServiceEnd, interval 31 day)
  and burned.offer_UID is null
  and burned_assets.predecessor_uid is null;
  
commit;


  -- ------------------------- --
  -- START Prep for next pass
  
  -- remove duplicates process --

  -- make sure we don't use same offer for more than one order/sn combination
  drop table if exists test.jjtmp_bluecoat_dups;
  drop table if exists test.jjtmp_bluecoat_dups2;

  create table test.jjtmp_bluecoat_dups as (
  select distinct book.offer_uid,book.`Order Number`,book.`Quote Serial Number`
    from test.jjtmp_Bluecoat_BookingFile book
    where offer_uid is not null)  ;
    
  commit;  

  create table test.jjtmp_bluecoat_dups2 as (select dup2.offer_uid
                            from test.jjtmp_bluecoat_dups dup2
                            group by dup2.offer_uid
                            having count(1) > 1);

  commit;
    
  -- remove any links if there were overlaps  
  update test.jjtmp_Bluecoat_BookingFile book 
  set book.offer_desc = null,
  book.offer_uid = null,
  book.opp_uid = null,
  book.predecessor_uid = null
  where coalesce(book.Booked,'') <> 'complete' 
  and exists (select dup.offer_uid from test.jjtmp_bluecoat_dups2 dup where dup.offer_uid = book.offer_uid)
    and `Order Number` = (select dup.`Order Number` from test.jjtmp_bluecoat_dups dup where dup.offer_uid = book.offer_uid limit 1);

  commit;
  

  -- END remove duplicates process --
 
  -- add to our burned offers list
  insert into test.jjtmp_bluecoat_burned_offers (offer_UID)
    select distinct book.offer_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  insert into test.jjtmp_bluecoat_burned_assets  (predecessor_UID)
    select distinct book.predecessor_UID
    from test.jjtmp_Bluecoat_BookingFile book;

  commit;
  
  
  -- END Prep for next pass
  -- ----------------------------- --

drop table if exists test.jjtmp_bluecoat_dups;
drop table if exists test.jjtmp_bluecoat_dups2;

  commit;
  
-- last status update to catch non-matched start dates, this is new as start date mis-matches were not important to the team
update test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`
inner join test.bluecoat_offer_quote_key offer
  on offer.offer_uid = book.offer_uid
set book.offer_DESC = concat('Date ',book.offer_DESC)
where coalesce(book.Booked,'') <> 'complete' 
  and book.offer_UID is not null
  and bsum.ServiceStart <> date(offer.offer_start)
  and book.offer_DESC not like '%Date%';

commit;


-- END OF MATCHING

--- YEEHAW!

