
-- list to HouseAccount (may not be needed in the future but run it anyway to be sure)

-- output to CSV for scrub file
select distinct opps._ID,'HA - Bad Data - BDT',opps.FLOWS_SALESSTAGES_STATE_NAME
from bluecoat.APP_OPPORTUNITIES opps
where opps.ISSUBORDINATE = 'undefined'
   and opps.FLOWS_SALESSTAGES_STATE_NAME not in ('transitioned','noService','houseAccount')
   and opps.DISPLAYNAME like '%Transitioned%'  ;  
   