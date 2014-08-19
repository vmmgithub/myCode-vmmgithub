DELIMITER ;;
CREATE DEFINER=`smukerji`@`10.100.%.%` PROCEDURE `SP_CoveredAsset`()
SQL SECURITY INVOKER
begin

 select 'End User Account' As `End User Account`,
'Asset #' As `Asset #`,
'Product' As `Product`,
'BusinessLine' As `BusinessLine`,
'PO#' As `PO#`,
'Serial #' As `Serial #`
union all

select `End User Account`,
`Asset #`,
`Product`,
`BusinessLine`,
`PO#`,
`Serial #`  from (
select distinct 
`End User Company (Opportunity)` As `End User Account`,
ifnull(`Existing Serial Number`,'') As `Asset #`,
case when S.`Covered Product`='0'  then '' else S.`Covered Product`end  As `Product`,
case when `SSI Business Line` ='WAN Optimization' then 'WANOPT'
when `SSI Business Line` ='BCWF NEW SERVICE' then 'bcwf'
when `SSI Business Line` ='AV' then 'AV'
else
replace(replace(lower(`SSI Business Line`),' ',''),'-','') end  As BusinessLine,
ifnull(poNumber ,'') As `PO#`,
ifnull(`Existing Serial Number`,'') As `Serial #`
 from AddAssets S
 where `Existing Serial Number` not in ('1',''))A;
 
 END;;
DELIMITER ;
