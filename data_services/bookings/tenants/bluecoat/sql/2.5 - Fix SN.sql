
-- find those new serial numbers that the zero might have been dropped
select distinct book.`Quote Serial Number` from test.jjtmp_Bluecoat_BookingFile book
left outer join bluecoat.APP_ASSETS asset
  on asset.DISPLAYNAME = book.`Quote Serial Number`
  and asset.TYPE = 'app.asset/service'
where book.offer_uid is null 
and asset._ID is null
and length(book.`Quote Serial Number`) < 10
and left(book.`Quote Serial Number`,1) in ('1','2','3','4','5','6','7','8','9');

-- check each SN above to see if there is an asset with a preceeding 0
select * from bluecoat.APP_ASSETS asset 
where asset.TYPE = 'app.asset/service' 
and asset.displayName like '%810086171%';

-- if one is found modify our booking file so we can match to it
update test.jjtmp_Bluecoat_BookingFile
set `Quote Serial Number` = '0313150071'
where `Quote Serial Number` = '313150071';

commit;



