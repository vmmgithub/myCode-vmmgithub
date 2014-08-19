DELIMITER ;;
CREATE DEFINER=`smukerji`@`10.10.%` PROCEDURE `SP_ServiceProduct`()
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
select distinct concat('"',`Existing Service Product`,'_Service"')  As `LoadID`,
'""' As `EXT:category`,
'""'  As `Description`,
concat('"',IfNull(`Existing Service Product` COLLATE utf8_bin,''),'"') As `DisplayName`,
concat('"',IfNull(`Existing Service Product` COLLATE utf8_bin,''),'"') As `EXT:sku`,
'""' As `Value@UnitPrice`,
'""' As `Code@UnitPrice`,
'"Service"' As `Type`

 from AddAssets;

end;;
DELIMITER ;
