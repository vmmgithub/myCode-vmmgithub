set tmp_table_size=6000000000;
set max_heap_table_size=6000000000;

drop table if exists test.bluecoat_offer_quote_key;
drop table if exists test.bluecoat_opp_quote_key;
drop table if exists test.primaryQuoteName;
drop table if exists test.latestQuote;
drop table if exists test.ValidBooking;

-- alter table bluecoat.RELATIONSHIPS add index idx_RELATIONSHIPS_main (SOURCETABLE,DESTTABLE,SOURCEKEY);

-- alter table bluecoat.APP_ASSETS add index idx_bluecoat_app_assets_name (DISPLAYNAME);


   -- dealing with lack of 'latest quote' causing grief
   create table test.primaryQuoteName as (
    select primaryQuoteName.sourcekey,max(primaryQuoteName.destname) as destname 
    from bluecoat.RELATIONSHIPS primaryQuoteName
    where primaryQuoteName.SOURCETABLE = 'APP_OPPORTUNITIES'
    and primaryQuoteName.DESTTABLE = 'APP_QUOTES'
    and primaryQuoteName.relname = 'quote'
    group by primaryQuoteName.sourcekey);
    
    commit;
    
    create table test.latestQuote as (
    select latestQuote.*
    from bluecoat.RELATIONSHIPS latestQuote
    inner join test.primaryQuoteName primaryQuote
      on primaryQuote.SOURCEKEY = latestQuote.SOURCEKEY
      and latestQuote.destname = primaryQuote.destname
      and latestQuote.SOURCETABLE = 'APP_OPPORTUNITIES'
      and latestQuote.DESTTABLE = 'APP_QUOTES'
      and latestQuote.RELNAME = 'quote');
      
    commit;
    
    alter table test.latestQuote add index latestQuote (SOURCEKEY);
    
       -- dealing with cancelled bookings causing grief
    create table test.ValidBooking as (
    select bookingRelation.*,booking.FLOWS_BOOKINGSTAGES_STATE_NAME as Status
    from bluecoat.RELATIONSHIPS bookingRelation
    inner join bluecoat.APP_BOOKINGS booking
      on booking._ID = bookingRelation.DESTKEY
    where coalesce(booking.FLOWS_BOOKINGSTAGES_STATE_NAME ,'') in ('discrepant', 'completed'));
    
    commit;
    
    alter table test.ValidBooking add index ValidBooking (SOURCEKEY);
    
-- ---------------------------------- --
-- generate an offer / quote key list --
-- ---------------------------------- --
create table test.bluecoat_offer_quote_key as (
select distinct
  offers._ID as offer_UID,
  offers.DISPLAYNAME as offer_name,
  offers.RESULT_NAME as result_name,
  product.DESTNAME as Relationship_Product,
  quotes.DESTKEY as Relationship_Quote_UID,
  offers.amount_amount as offer_amount,
  offers.amount_code_name as offer_currency,
  offers.startdate as offer_start,
  offers.enddate as offer_end,
  predecessor.DESTKEY as Relationship_Predecessor_UID,
  offers.ISEXCLUDED
from bluecoat.APP_OFFERS offers
left outer join bluecoat.RELATIONSHIPS product
    on product.SOURCETABLE = 'APP_OFFERS'
    and product.DESTTABLE = 'APP_PRODUCTS'
    and product.SOURCEKEY = offers._ID
left outer join bluecoat.RELATIONSHIPS quotes
    on quotes.SOURCETABLE = 'APP_OFFERS'
    and quotes.DESTTABLE = 'APP_QUOTES'  
    and quotes.SOURCEKEY = offers._ID
left outer join bluecoat.RELATIONSHIPS predecessor
    on predecessor.SOURCETABLE = 'APP_OFFERS'
    and predecessor.DESTTABLE = 'APP_ASSETS'
    and predecessor.RELNAME = 'predecessor'
    and predecessor.SOURCEKEY = offers._ID    ) ;

commit;
    
        
-- ---------------------------------------- --
-- generate an opportunity / quote key list --
-- ---------------------------------------- --

create table test.bluecoat_opp_quote_key as (
select distinct
  opps._ID as opp_UID,
  opps.DISPLAYNAME as opp_name,
  contact.DESTNAME as Relationship_Company,
  opps.FLOWS_SALESSTAGES_STATE_NAME as SalesStage,
  coalesce(coalesce(primaryQuote.DESTKEY,latestQuote.DESTKEY),baseQuote.DESTKEY) as useQuote_UID,
  opps.AMOUNT_AMOUNT as Amount,
  opps.AMOUNT_CODE_NAME as Currency,
  booking.DESTKEY as Relationship_Booking_UID,
  primaryQuote.DESTKEY as Relationship_primaryQuote_UID,
  latestQuote.DESTKEY as Relationship_latestQuote_UID,
  baseQuote.DESTKEY as Relationship_baseQuote_UID,
  opps.SYSTEMPROPERTIES_CREATEDON as CreatedOn
from bluecoat.APP_OPPORTUNITIES opps
left outer join bluecoat.RELATIONSHIPS primaryQuote
    on primaryQuote.SOURCETABLE = 'APP_OPPORTUNITIES'
    and primaryQuote.DESTTABLE = 'APP_QUOTES'
    and primaryQuote.SOURCEKEY = opps._ID
    and primaryQuote.RELNAME = 'primaryQuote'
left outer join bluecoat.RELATIONSHIPS baseQuote
    on baseQuote.SOURCETABLE = 'APP_OPPORTUNITIES'
    and baseQuote.DESTTABLE = 'APP_QUOTES'
    and baseQuote.SOURCEKEY = opps._ID
    and baseQuote.RELNAME = 'baseQuote'
left outer join test.latestQuote latestQuote
    on latestQuote.SOURCETABLE = 'APP_OPPORTUNITIES'
    and latestQuote.DESTTABLE = 'APP_QUOTES'
    and latestQuote.SOURCEKEY = opps._ID
    and latestQuote.RELNAME = 'quote'
left outer join bluecoat.RELATIONSHIPS contact
    on contact.SOURCETABLE = 'APP_OPPORTUNITIES'
    and contact.DESTTABLE = 'CORE_CONTACTS'
    and contact.SOURCEKEY = opps._ID
    and contact.relname = 'customer'
 left outer join test.ValidBooking booking
    on booking.SOURCETABLE = 'APP_OPPORTUNITIES'
    and booking.DESTTABLE = 'APP_BOOKINGS'
    and booking.SOURCEKEY = opps._ID
  where opps.ISSUBORDINATE <> 'true'
   and opps.FLOWS_SALESSTAGES_STATE_NAME not in ('transitioned','consolidated')
   and opps.DISPLAYNAME not like '%Transitioned%');  

commit;


-- create our indexes for faster processing --

alter table test.bluecoat_offer_quote_key add index idx_bluecoat_offer_quote_key_Relationship_Quote_UID (Relationship_Quote_UID);

alter table test.bluecoat_offer_quote_key add index idx_bluecoat_offer_quote_key_offer_name (offer_name);

alter table test.bluecoat_offer_quote_key add index idx_bluecoat_offer_quote_key_offer_UID (offer_UID);

alter table test.bluecoat_offer_quote_key add index idx_bluecoat_offer_quote_key_offer_predecessor (relationship_predecessor_UID);

alter table test.bluecoat_opp_quote_key add index idx_bluecoat_offer_quote_key_opp_UID (opp_UID);

alter table test.bluecoat_opp_quote_key add index idx_bluecoat_offer_quote_key_useQuote_UID (useQuote_UID);

commit;

-- select * from test.bluecoat_opp_quote_key limit 10
-- select * from test.bluecoat_offer_quote_key limit 10


-- only want the most current base offer otherwise we'll be messed up with our splits still reporting the last Opportunity

drop table if exists test.jjtmp_DupFilter;

commit;

create table test.jjtmp_DupFilter as (
    select distinct 
    offer.offer_uid,
    opp.opp_UID,
    opp.createdOn,
    offer.result_name,
    offer.Relationship_Predecessor_UID,
    offer.Relationship_Quote_UID
    from test.bluecoat_opp_quote_key opp
    inner join test.bluecoat_offer_quote_key offer
      on offer.Relationship_Quote_UID = opp.relationship_basequote_UID 
    where offer.offer_uid in ( select
          offer2.offer_uid
          from test.bluecoat_opp_quote_key opp2
          inner join test.bluecoat_offer_quote_key offer2
            on offer2.Relationship_Quote_UID = opp2.relationship_basequote_UID 
          group by offer2.offer_uid
          having count(1) > 1)
    order by offer.offer_uid,opp.createdOn ) ;

    alter table test.jjtmp_DupFilter add index idx_jjtmp_DupFilter (offer_uid);

    commit;

update test.bluecoat_offer_quote_key offer
set offer.result_name = 'DoNotUse'
where offer.Relationship_Quote_UID = (select bk.Relationship_Quote_UID
              from test.jjtmp_DupFilter bk 
              where offer.offer_uid = bk.offer_uid 
              limit 1);

commit;

delete from test.bluecoat_offer_quote_key where result_name = 'DoNotUse';

drop table if exists test.primaryQuoteName;
drop table if exists test.latestQuote;
drop table if exists test.ValidBooking;
commit;

  