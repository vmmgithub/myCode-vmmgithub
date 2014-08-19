set tmp_table_size=6000000000;
set max_heap_table_size=6000000000;

drop table if exists test.bluecoat_opp_partner_quote_key;
drop table if exists test.primaryQuoteName;
drop table if exists test.latestQuote;
drop table if exists test.ValidBooking;


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
    where coalesce(booking.FLOWS_BOOKINGSTAGES_STATE_NAME ,'') = 'completed');
    
    commit;
    
    alter table test.ValidBooking add index ValidBooking (SOURCEKEY);
    
    
        
-- ---------------------------------------- --
-- generate a partner opportunity / quote key list --
-- ---------------------------------------- --

create table test.bluecoat_opp_partner_quote_key as (
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
  where opps.ISSUBORDINATE = 'true'
   and opps.FLOWS_SALESSTAGES_STATE_NAME not in ('transitioned','consolidated')
   and opps.DISPLAYNAME not like '%Transitioned%');  

commit;


-- create our indexes for faster processing --

alter table test.bluecoat_opp_partner_quote_key add index idx_bluecoat_opp_partner_quote_key_opp_UID (opp_UID);

alter table test.bluecoat_opp_partner_quote_key add index idx_bluecoat_opp_partner_quote_key_useQuote_UID (useQuote_UID);

commit;

drop table if exists test.primaryQuoteName;
drop table if exists test.latestQuote;
drop table if exists test.ValidBooking;
commit;

  