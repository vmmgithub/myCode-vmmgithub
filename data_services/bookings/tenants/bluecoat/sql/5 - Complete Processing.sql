
-- complete our records so we leave them alone
drop table if exists test.jjtmp_closing;

create table test.jjtmp_closing as (
select close.OppName
from test.jjtmp_bluecoat_Closures close
where salesStage in ('closedSale') 
      and not exists (select distinct split.opp_uid 
                      from test.jjtmp_Bluecoat_Splits split 
                      where split.result = 'split'
                      and close.oppName = split.opp_uid)
           and not exists (select book.opp_UID 
                      from test.jjtmp_Bluecoat_BookingFile book
                      where book.opp_UID = close.oppName
                        and book.offer_DESC <> 'Exact')
group by close.OppName);

commit;

update test.jjtmp_Bluecoat_BookingFile book
set book.booked = 'complete'
where book.opp_UID in (select close.oppname 
                        from test.jjtmp_closing close) ;

commit;

-- mark those we want to close immediately because they are exact matches and not splitting

drop table if exists test.jjtmp_closing;

create table test.jjtmp_closing as (
select close.OppName
from test.jjtmp_bluecoat_Closures close
where salesStage in ('poReceived','customerCommitment','quoteCompleted','quoteRequested','quoteDelivered','contacted','notContacted') 
      and not exists (select distinct split.opp_uid 
                      from test.jjtmp_Bluecoat_Splits split 
                      where split.result = 'split'
                      and close.oppName = split.opp_uid)
      and not exists (select book.opp_UID 
                      from test.jjtmp_Bluecoat_BookingFile book
                      where book.opp_UID = close.oppName
                        and book.offer_DESC <> 'Exact')
group by close.OppName);

commit;

update test.jjtmp_Bluecoat_BookingFile book
set book.booked = 'next'
where coalesce(book.booked,'') <> 'complete' 
  and book.opp_UID in (select close.oppname 
                        from test.jjtmp_closing close ) ;

commit;

drop table if exists test.jjtmp_closing;
