
-- find those assets missing alltogether - add in dates/product comparison
-- ignore those with offers as we know they have assets

-- output to CSV for scrub file
select distinct
  book.region as clienttheatre,	
  book.`end user country` as country,	
  book.`end user` as 'End User Company (Opportunity)',	
  book.`Quote Serial Number` as 'Existing Serial Number',	
  bsum.Product as 'Existing Service Product',	
  bsum.`Part Number` as  'Covered Product'	,
  'Adds' as 'Batch Type',	
  bl.`Business Line` as 'SSI Business Line',
  bsum.ServiceStart as 'Existing End Date',	
  case
    when length(trim(book.`Booked Date`)) = 0 then book.`Invoice Date`
    when book.`Booked Date` is not null then book.`Booked Date`
    else book.`Invoice Date`
  end resolutionDate	,
  bsum.value as poAmount,	
  book.`Purchase Order Number` as poNumber,	
  case
    when length(trim(book.`Ordered Date`)) = 0 then book.`Invoice Date`
    when book.`Ordered Date` is not null then book.`Ordered Date`
    else book.`Invoice Date`  
  end poDate	,
  '' reason	,
  bsum.value as 'SO Amount',	
  case
    when length(trim(book.`Booked Date`)) = 0 then book.`Invoice Date`
    when book.`Booked Date` is not null then book.`Booked Date`
    else book.`Invoice Date`
  end 'soDate',	  
  book.`Order Number` as 'SO Number',	
  'FY14Q4' as 'Target Selling Period',	
  bsum.value as 'Local Amount',	
  'usd' as 'Local Currency' ,asset.DISPLAYNAME , rel.relname
from test.jjtmp_Bluecoat_BookingFile book
inner join test.jjtmp_bluecoat_SUM bsum
  on bsum.`Order Number` = book.`Order Number`
  and bsum.`Quote Serial Number` = book.`Quote Serial Number`  
left outer join test.jjtmp_bluecoat_reference_BusinessLine bl
  on bsum.product = bl.ExistingServiceProduct  
left outer join bluecoat.APP_ASSETS asset
  on asset.DISPLAYNAME = book.`Quote Serial Number`
  and asset.TYPE = 'app.asset/service'
left outer join bluecoat.RELATIONSHIPS rel
  on rel.DESTTABLE = 'APP_ASSETS'
  and rel.DESTKEY = asset._ID
  and rel.RELNAME = 'predecessor'
where 
  book.offer_uid is null and (asset.DISPLAYNAME is null or rel.relname is not null);
 
  