DELIMITER ;;
CREATE DEFINER=`smukerji`@`10.10.%` PROCEDURE `SP_ServiceAsset`()
SQL SECURITY INVOKER
begin

select 
'End User Account',
'Serial #' ,
'BusinessLine' ,
'PO#',
'Asset #' ,
'Product' ,
'Bill to Geo Zone' ,
'End User Account Country' ,
'Entitlement End Date' ,
'Entitlement Start Date' ,
'ClientBatchQuarter' ,
'Region' ,
'Territory',
'Amount' ,
'sonumber' ,
'Batch Type' ,
'ExternalEnddate' 
union all
select  `End User Account`,`Serial #`,`BusinessLine`,`PO#`,`Asset #`,`Product`,`Bill to Geo Zone`,`End User Account Country`,
`Entitlement End Date`,`Entitlement Start Date`,`ClientBatchQuarter`,`Region`,`Territory`,`Amount`,sonumber,`Batch Type` ,
ExternalEnddate  from (
select  @row_num := IF(@prev_value=concat_ws('',`Serial #`,`ExternalEnddate`),@row_num+1,1) AS RowNumber,
@prev_value := concat_ws('',`Serial #`,`ExternalEnddate`) ,`End User Account`,`Serial #`,`BusinessLine`,`PO#`,`Asset #`,`Product`,`Bill to Geo Zone`,`End User Account Country`,
`Entitlement End Date`,`Entitlement Start Date`,`ClientBatchQuarter`,`Region`,`Territory`,`Amount`,sonumber,`Batch Type` ,
ExternalEnddate  from (
select distinct 
concat('"',`End User Company (Opportunity)` ,'"') As `End User Account`,
 
concat('"',ifnull(S.`Existing Serial Number`,''),'"') As `Serial #`,
concat('"',case when `SSI Business Line` ='WAN Optimization' then 'WANOPT'
when `SSI Business Line` ='BCWF NEW SERVICE' then 'bcwf'
when `SSI Business Line` ='AV' then 'AV'
else
replace(replace(lower(`SSI Business Line`),' ',''),'-','') end,'"')  As BusinessLine,
concat('"',ifnull(poNumber,''),'"') As `PO#`,
concat('"',ifnull(S.`Existing Serial Number`,''),'"') As `Asset #`,
concat('"',ifnull(`Existing Service Product`,''),'"') as 'Product',
case when S.clienttheatre='EMEA' then '"eumea"' when S.clienttheatre='APAC' then '"asiaPacific"' 
when S.clienttheatre='NALA' or S.clienttheatre='AMERICAS'  then '"nala"'  
end  as `Bill to Geo Zone`,
case when S.country='Taiwan' then '"TW"' 
when S.country='United States Virgin Islands' then '"USVI"'
when S.country='Venezuela' then '"VE"'
when S.country='Bolivia' then '"BO"'
when S.country='Tanzania' then '"TZ"'
when S.country='United States of America' then '"US"' 
when S.country='R?union' then '"RE"' 
else  concat('"',ifnull(C1.`ROD name`,S.country),'"') end  As `End User Account Country`,

Case When `Existing End Date` not in ('','{}') Then 
			(concat('"',ifnull(concat(cast(date_format(concat(concat(concat(right(replace(`Existing End Date`,' 0:00',''),4),'-'),replace(left(`Existing End Date`,2),'/',''),'-'),
replace(substring(`Existing End Date`,locate('/',`Existing End Date`,1),3),'/','')),'%Y-%m-%d') as char),'T12:00:00.000Z'),''),'"'))
			Else '""' End as `Entitlement End Date`,
      
Case When `Existing End Date` not in ('','{}') Then 
			(concat('"',ifnull(concat(cast(DATE_SUB(date_format(concat(concat(concat(right(replace(`Existing End Date`,' 0:00',''),4),'-'),replace(left(`Existing End Date`,2),'/',''),'-'),
replace(substring(`Existing End Date`,locate('/',`Existing End Date`,1),3),'/','')),'%Y-%m-%d'),INTERVAL 365 DAY) as char),'T12:00:00.000Z'),''),'"'))
			Else '""' End as `Entitlement Start Date`,
      
concat('"',concat(right(`Target Selling Period`,2),
concat('-20',replace(replace(`Target Selling Period`,right(`Target Selling Period`,2),''),'FY',''))),'"') As `ClientBatchQuarter`,
concat('"',ifnull(L.`ROD name` ,''),'"') As `Region`,
concat('"',ifnull(L1.`ROD name`,''),'"')  As Territory,
concat('"',ifnull(`Local Amount`,''),'"') 'Amount',
concat('"',ifnull(`SO Number`,''),'"') As sonumber,
concat('"',ifnull(lower(`Batch Type`) ,''),'"') As 'Batch Type',
 concat('"',ifnull(`Existing End Date`,''),'"') As 'ExternalEnddate'
from AddAssets S 
left join `BlueCoat-core.lookups` C1 on (C1.`ROD name`=country or C1.`name`=country) and C1.`Group`='Country'
left join `Master Country to Region` R1 on R1.Country=C1.Name 
left join `NALA Territory` T on T.Country=C1.Name
left join `BlueCoat-app.lookups` L on L.Name=R1.Region and L.Group1='ClientRegion'
left join `BlueCoat-app.lookups` L1 on L1.Name=T.Territory and L1.Group1='ClientTerritory'
where `Existing Serial Number`not in ('1',''))A,
(SELECT @row_num := 1) x,
         (SELECT @prev_value := '') y 
         order by `Serial #`,`ExternalEnddate`)D
where RowNumber=1

;
end;;
DELIMITER ;
