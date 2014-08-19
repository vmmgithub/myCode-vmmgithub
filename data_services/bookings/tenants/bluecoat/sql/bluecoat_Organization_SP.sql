DELIMITER ;;
CREATE DEFINER=`smukerji`@`10.10.%` PROCEDURE `SP_Organization`()
SQL SECURITY INVOKER
begin

select 'LoadID',
'ContactNumbers:Business' ,
'Name' ,
'donot mail' ,
'OnlineAddresses:PrimaryEmail' ,
'Channel Tier',
'OnlineAddresses:CorporateWebsite' ,
'billing',
'business ' ,
'LoadID:Addresses:Location' ,
'primarycontact (relationship)',
'SecondaryContact',
'shipping',
'Subset',
'LoadID:Organizations:Parent'

union all

select distinct concat('"',`End User Company (Opportunity)`,'"'),
'""',
concat('"',`End User Company (Opportunity)`,'"'),
'""' as `donot mail`,
'""' as `OnlineAddresses:PrimaryEmail`,
'""' as `Channel Tier`,
'""' as `OnlineAddresses:CorporateWebsite`,
'""' as `billing`,
'""' as `business `,
'""' as `LoadID:Addresses:Location`,
'""' as `primarycontact (relationship)`,
'""' as `SecondaryContact`,
'""' as `shipping`,
'""' as `Subset`,
'""' as `LoadID:Organizations:Parent`
from AddAssets where `End User Company (Opportunity)` <>'';

end;;
DELIMITER ;
