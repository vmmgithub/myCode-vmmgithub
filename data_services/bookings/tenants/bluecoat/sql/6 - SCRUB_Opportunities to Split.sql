
----------------------------------
-- Opportunity Split Scrub File --
----------------------------------
-- need both the base offer UID and the current in the scrub file as it is variable which one works.
drop table test.jjtmp_working_splits;

commit;

create table test.jjtmp_working_splits as (
select split.opp_uid,split.offer_UID as Move_Offers
from test.jjtmp_Bluecoat_Splits split
where result = 'split'   );

insert into test.jjtmp_working_splits (opp_uid,move_offers)
select split.opp_uid,split.offer_UID_to_Split as Move_Offers
from test.jjtmp_Bluecoat_Splits split
where result = 'split'  ;

commit;

-- output to CSV for scrub file
select split.opp_uid,CONCAT_WS(',', GROUP_CONCAT(DISTINCT split.Move_Offers)) as Move_Offers
from test.jjtmp_working_splits split
group by split.opp_uid 
order by 1;

