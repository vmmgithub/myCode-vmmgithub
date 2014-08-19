
----------------------------------
-- Opportunity Close Scrub File --
----------------------------------

-- output to CSV for scrub file
select close.OppName,
  max(close.resolutionDate) resolutionDate,	
  sum(close.poAmount)	poAmount,
  max(close.poDate)	poDate,
  CONCAT_WS(', ', GROUP_CONCAT(DISTINCT close.poNumber)) as poNumber,	
  'csRAP' as 'reason'	,
  sum(close.soAmount) soAmount,	
  max(close.soDate) soDate,
  CONCAT_WS(', ', GROUP_CONCAT(DISTINCT close.soNumber)) as soNumber
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
group by close.OppName;



