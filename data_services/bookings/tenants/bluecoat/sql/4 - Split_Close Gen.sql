
-- creating a bunch of temp tables for the following SCRUB scripts to use

-- ----------------- --
-- FIND OUT WHICH OPPS NEED TO SPLIT --

drop table if exists test.jjtmp_Bluecoat_Splits;
drop table if exists test.jjtmp_filterfile;

-- grab a filtered list so we are only looking at opps we are using in our process
create table test.jjtmp_filterfile as (
select distinct
  opp.*
from test.bluecoat_opp_quote_key opp
inner join test.jjtmp_Bluecoat_BookingFile filterfile
  on opp.opp_UID = filterfile.opp_UID 
where coalesce(filterfile.booked,'') <> 'complete' );
  
  commit;

-- split list creation
-- grab every offer that exists on our opportunities
create table test.jjtmp_Bluecoat_Splits as (
select distinct
  opp.opp_uid,
  offer_base.offer_UID as offer_UID_to_Split,
  offer_main.offer_UID,
  offer_base.Relationship_predecessor_UID,
  opp.salesStage ,
  'split' as result
from test.jjtmp_filterfile opp
inner join test.bluecoat_offer_quote_key offer_base
  on offer_base.Relationship_Quote_UID = opp.Relationship_baseQuote_UID 
inner join test.bluecoat_offer_quote_key offer_main
  on offer_main.Relationship_predecessor_UID = offer_base.Relationship_predecessor_UID    );

commit;

alter table test.jjtmp_Bluecoat_Splits add index jjtmp_Bluecoat_Splits_step1_offer_uid (offer_UID);

alter table test.jjtmp_Bluecoat_Splits add index idx_jjtmp_bluecoat_splits_opp (opp_uid);

alter table test.jjtmp_Bluecoat_Splits add index idx_jjtmp_bluecoat_splits_Relationship_predecessor_UID (Relationship_predecessor_UID);

-- mark those offers we will book (remaining are marked with 'split')
update test.jjtmp_Bluecoat_Splits splits
  inner join test.jjtmp_Bluecoat_BookingFile book
    on book.predecessor_UID = splits.Relationship_predecessor_UID
    set splits.result = 'book';

commit;


drop table if exists test.jjtmp_filterfile;

-- --------------------------------------------------------------- --
-- Sum the orders by what is on each opportunity for close numbers --

drop table if exists test.jjtmp_bluecoat_SUM_Order;

create table test.jjtmp_bluecoat_SUM_Order as (
select book.`Order Number`,book.opp_UID,
  sum(book.`Selling Price`) as Value
from test.jjtmp_Bluecoat_BookingFile book
group by book.`Order Number`,book.opp_UID);

commit;

-- FIND OUT WHICH OPPS CAN CLOSE

drop table if exists test.jjtmp_bluecoat_Closures;

create table test.jjtmp_bluecoat_Closures as (
select distinct
  book.opp_uid 'OppName',
  case
    when book.`Ordered Date` is null or book.`Ordered Date` = '' then book.`Invoice Date`
    else book.`Ordered Date`
  end 'resolutionDate',	
  bsum.value as 'poAmount'	,
  case
    when book.`Ordered Date` is null or book.`Ordered Date` = '' then book.`Invoice Date`
    else book.`Ordered Date`
  end 'poDate'	,
  book.`Purchase Order Number` as 'poNumber',	
  'csRAP' as 'reason'	,
  bsum.value as 'soAmount',	
  case
    when book.`Booked Date` is null or book.`Booked Date` = '' then book.`Invoice Date`
    else book.`Booked Date` 
  end 'soDate',
  book.`Order Number` as 'soNumber',
  opp.SalesStage
from test.jjtmp_Bluecoat_BookingFile book 
inner join test.jjtmp_bluecoat_SUM_Order bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.Opp_UID = book.Opp_UID
inner join test.bluecoat_opp_quote_key opp
  on book.opp_UID = opp.opp_UID
where 1=1
  and not exists (select distinct split.opp_uid 
                      from test.jjtmp_Bluecoat_Splits split 
                      where split.result = 'split'
                      and book.opp_uid = split.opp_uid));

commit;

drop table if exists test.jjtmp_bluecoat_opp_value;

create table test.jjtmp_bluecoat_opp_value as (
select close.OppName ,
  sum(close.soAmount) New_OppAmount	
from test.jjtmp_bluecoat_Closures close
group by close.OppName);

commit;
