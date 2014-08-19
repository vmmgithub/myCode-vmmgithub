DELIMITER ;;
CREATE DEFINER=`smukerji`@`10.10.%` PROCEDURE `SP_CoveredProduct`()
SQL SECURITY INVOKER
begin

select 'LoadID',
'EXT:category',
'Description',
'DisplayName',
'EXT:sku',
'Value@UnitPrice',
'Code@UnitPrice',
'Type' 
union all
select distinct concat('"',`Covered Product`,'"') As `LoadID`,
'""' As `EXT:category`,
'""' As `Description`,
concat('"',`Covered Product`,'"') As `DisplayName`,
'""' As `EXT:sku`,
'""' As `Value@UnitPrice`,
'""' As `Code@UnitPrice`,
'"covered"' As `Type`
from AddAssets where `Covered Product`<>'';


end;;
DELIMITER ;
