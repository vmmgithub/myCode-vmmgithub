DROP PROCEDURE IF EXISTS Obsidian.spDqMainvalidation;
CREATE PROCEDURE Obsidian.`spDqMainvalidation`(IN TENANT VARCHAR(50))
BEGIN
    
   DECLARE CONTINUE HANDLER FOR SQLSTATE  '21S01'
   INSERT INTO DQ_ERRORS SELECT TENANT,'','Column count doesn''t match value count';
   DECLARE CONTINUE HANDLER FOR SQLSTATE '42S22'
   INSERT INTO DQ_ERRORS SELECT TENANT,'','Unknown Column';
 BEGIN
call spAssetValidation_Casedyn(TENANT,'COVERED_ASSETS');
call spAssetValidation_Casedyn(TENANT,'SERVICE_ASSETS');
call spOfferValidation_Casedyn(TENANT);
call spOpportunityValidation_Casedyn(TENANT);
  END;
END;

--------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS Obsidian.spOfferValidation_Casedyn;
CREATE PROCEDURE Obsidian.`spOfferValidation_Casedyn`(IN TENANT varchar(40))
BEGIN

  SET @DQSTR="";
  SET @STRING="";
  SET @ASSETSTRING="";
  
SET @COUNT=CONCAT(
    " SELECT COUNT(1) INTO @ASSTCOUNT FROM ",TENANT,".APP_OFFERS ; " );
    
    PREPARE DV_STMT3 FROM @COUNT;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3; 

drop table if exists OfferValidation_Dyn;
SET @CONSTRING=CONCAT(" CREATE TABLE OfferValidation_Dyn AS ( SELECT DISTINCT  ");
SET @CONSTRING4=" CASE WHEN " ;
SET @CONSTRING1= " THEN ";
SET @CONSTRING2= " WHEN ";
SET @CONSTRING3= " ELSE 'NA' END AS ";

 SET @MIN=(SELECT MIN(id) FROM ValidationConfiguration WHERE  EntityName='Offer');
 SET @MAX=(SELECT MAX(id) FROM ValidationConfiguration WHERE  EntityName='Offer');
 OUTERLOOP: WHILE @MIN<=@MAX DO
 
  SET @RULEDES= (SELECT RuleDescription FROM ValidationConfiguration WHERE ID=@MIN AND EntityName='Offer');
  SET @RULENAME= (SELECT RuleName FROM ValidationConfiguration WHERE ID=@MIN AND EntityName='Offer');
  SET @COLNAME =  (SELECT ColumnName FROM ValidationConfiguration WHERE ID=@MIN AND EntityName='Offer');
  SET @STRING=CONCAT (ifnull(@STRING,''),@CONSTRING4,@RULEDES,@CONSTRING1,"'",@RULENAME,"'",@CONSTRING3,"'",@COLNAME,@RULENAME,"'",",");
  SET @MIN=@MIN+1;
  END WHILE;

  SET @ASSETSTRING= CONCAT(@CONSTRING,@STRING, 
  "( SELECT  'M.102' FROM ",TENANT,".APP_QUOTES WHERE _ID<>Q.DESTKEY )as 'quote',",
  " CASE WHEN Q.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'quotem101',",
  "( SELECT  'M.102' FROM ",TENANT,".APP_CONTACTS WHERE _ID<>C.DESTKEY )as 'customer',",
  " CASE WHEN C.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'customerm101',",
  "( SELECT  'M.102' FROM ",TENANT,".APP_PRODUCTS WHERE _ID<>P.DESTKEY )as 'product',",
  " CASE WHEN P.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'productm101',",
  "( SELECT  'M.102' FROM ",TENANT,".SERVICE_ASSETS WHERE _ID<>PR.DESTKEY )as 'predecessor',",
  " CASE WHEN PR.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'predecessorm101',",
 
  "( SELECT 'M.101' FROM ",TENANT,".RELATIONSHIPS R INNER JOIN ",TENANT,".SERVICE_ASSETS A ON A._ID=R.DESTKEY AND R.RELNAME='predecessor'",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS R1 ON R1.DESTKEY=R.DESTKEY AND R.RELNAME='reseller' WHERE R.SOURCEKEY=E.SOURCEKEY AND R.DESTKEY IS NULL ) as 'reseller',",
 
  "( SELECT 'M.101' FROM ",TENANT,".RELATIONSHIPS R INNER JOIN ",TENANT,".SERVICE_ASSETS A ON A._ID=R.DESTKEY AND R.RELNAME='predecessor'",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS R1 ON R1.DESTKEY=R.DESTKEY AND R.RELNAME='distributor' WHERE R.SOURCEKEY=E.SOURCEKEY AND R.DESTKEY IS NULL) as 'distributor',_ID ",
 
  " FROM ",TENANT, ".APP_OFFERS A ",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS E on E.SOURCEKEY=A._ID",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS Q ON Q.SOURCEKEY=A._ID AND Q.SOURCETABLE='APP_ASSETS' AND Q.RELNAME='quote'",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS C ON C.SOURCEKEY=A._ID AND C.SOURCETABLE='APP_ASSETS' AND C.RELNAME='customer'",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS P ON P.SOURCEKEY=A._ID AND P.SOURCETABLE='APP_ASSETS' AND P.RELNAME='product'",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS PR ON PR.SOURCEKEY=A._ID AND PR.SOURCETABLE='APP_ASSETS' AND PR.RELNAME='predecessor');"
      
      );

    PREPARE DV_STMT3 FROM  @ASSETSTRING;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3;

  SET @EXSTRING=" INSERT INTO DQ_METRICS (TENANT,`OBJECT TYPE`,ATTRIBUTE,`RULE #`,`TOTAL # RECORDS`,`FAILED # RECORDS`,`FAILURE LEVEL`,DATETIME) ";
  drop table if exists SchemaColumn ;
  Create table  SchemaColumn as (SELECT Ordinal_position,column_name FROM information_schema.columns where table_schema='Obsidian' and table_name='OfferValidation_Dyn');
  delete from SchemaColumn where column_name not like '%.%';
  
  SET @COLCOUNT = (select count(1) from SchemaColumn);
  SET @STR1= " SELECT ";
  SET @STR2= " 'Offer' ";
  SET @STR3= " ( SELECT COUNT(";
  SET @STR4= " ) FROM OfferValidation_Dyn where ";
  SET @STR5= " ='";
  SET @STR6= "'), ";
  SET @STR7= "',NOW()";
  SET @STR8= "  UNION ALL ";
  
  LOOPIN: WHILE @COLCOUNT >0 DO
    SET @COLUMN=(SELECT column_name FROM SchemaColumn WHERE Ordinal_position=@COLCOUNT );
    
    IF @COLUMN<>""
    THEN 
    SET @ATTRIBUTE=(SELECT CASE WHEN INSTR(column_name,'.')>1 THEN SUBSTRING( column_name,1,INSTR(column_name,'.')-2) ELSE column_name END FROM SchemaColumn WHERE Ordinal_position= @COLCOUNT);
    SET @RULE=(SELECT  CASE WHEN INSTR(column_name,'.')>1 THEN SUBSTRING( column_name,INSTR(column_name,'.')-1,length(column_name)) ELSE column_name END FROM SchemaColumn WHERE Ordinal_position=@COLCOUNT);
    SET @LVL =(SELECT Lvl FROM ValidationConfiguration WHERE ColumnName=@ATTRIBUTE AND RuleName=@RULE AND EntityName='Offer');  
    SET @DQSTR=CONCAT(IFNULL(@DQSTR,''),@STR1,"'",TENANT,"'",",",@STR2,",'",@ATTRIBUTE,"','",ifnull(@RULE,''),"',",@ASSTCOUNT,",",@STR3,"`",@COLUMN,"`",@STR4,"`",@COLUMN,"`",@STR5,@RULE,@STR6,"'",@LVL,@STR7,@STR8  );
  
    SET @COLCOUNT =@COLCOUNT -1;
  
    END IF;
  END WHILE ;

    SET @EXCMD= CONCAT( @EXSTRING,@DQSTR,
    " SELECT '", TENANT,"','Offer','quote','M.102',",@ASSTCOUNT,",( SELECT COUNT(`quote`) FROM OfferValidation where `quote`='M.102') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Offer','quote','M.101',",@ASSTCOUNT,",( SELECT COUNT(quotem101) FROM OfferValidation where quotem101='M.101') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Offer','customer','M.102',",@ASSTCOUNT,",( SELECT COUNT(customer) FROM OfferValidation where customer='M.102') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Offer','customer','M.101',",@ASSTCOUNT,",( SELECT COUNT(customerm101) FROM OfferValidation where customerm101='M.101') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Offer','Product','M.102',",@ASSTCOUNT,",( SELECT COUNT(Product) FROM OfferValidation where Product='M.102') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Offer','Product','M.101',",@ASSTCOUNT,",( SELECT COUNT(Productm101) FROM OfferValidation where Productm101='M.101') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Offer','predecessor','M.102',",@ASSTCOUNT,",( SELECT COUNT(predecessor) FROM OfferValidation where predecessor='M.102') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Offer','predecessor','M.101',",@ASSTCOUNT,",( SELECT COUNT(predecessorm101) FROM OfferValidation where predecessorm101='M.101') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Offer','Reseller','M.102',",@ASSTCOUNT,",( SELECT COUNT(reseller) FROM OfferValidation where reseller='M.102') ,'WARN',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Offer','Distributor','M.101',",@ASSTCOUNT,",( SELECT COUNT(distributor) FROM OfferValidation where distributor='M.101') ,'WARN',NOW();"
  );
  

  PREPARE DV_STMT3 FROM  @EXCMD;
  EXECUTE DV_STMT3;
  DEALLOCATE PREPARE DV_STMT3;
  
  SET @DQSTR="";
  SET @STRING="";
  SET @ASSETSTRING="";
   
  
END;

---------------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS Obsidian.spOpportunityValidation_Casedyn;
CREATE PROCEDURE Obsidian.`spOpportunityValidation_Casedyn`(IN TENANT varchar(40))
BEGIN

  SET @DQSTR="";
  SET @STRING="";
  SET @ASSETSTRING="";
  
SET @COUNT=CONCAT(
    " SELECT COUNT(1) INTO @ASSTCOUNT FROM ",TENANT,".APP_OPPORTUNITIES ; " );
    
    PREPARE DV_STMT3 FROM @COUNT;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3; 

drop table if exists OpportunityValidation_Dyn;
SET @CONSTRING=CONCAT(" CREATE TABLE OpportunityValidation_Dyn AS ( SELECT DISTINCT  ");
SET @CONSTRING4=" CASE WHEN " ;
SET @CONSTRING1= " THEN ";
SET @CONSTRING2= " WHEN ";
SET @CONSTRING3= " ELSE 'NA' END AS ";

 SET @MIN=(SELECT MIN(id) FROM ValidationConfiguration WHERE  EntityName='Opportunity');
 SET @MAX=(SELECT MAX(id) FROM ValidationConfiguration WHERE  EntityName='Opportunity');
 OUTERLOOP: WHILE @MIN<=@MAX DO
 
  SET @RULEDES= (SELECT RuleDescription FROM ValidationConfiguration WHERE ID=@MIN AND EntityName='Opportunity');
  SET @RULENAME= (SELECT RuleName FROM ValidationConfiguration WHERE ID=@MIN AND EntityName='Opportunity');
  SET @COLNAME =  (SELECT ColumnName FROM ValidationConfiguration WHERE ID=@MIN AND EntityName='Opportunity');
  SET @STRING=CONCAT (ifnull(@STRING,''),@CONSTRING4,@RULEDES,@CONSTRING1,"'",@RULENAME,"'",@CONSTRING3,"'",@COLNAME,@RULENAME,"'",",");
  SET @MIN=@MIN+1;
  END WHILE;

  SET @ASSETSTRING= CONCAT(@CONSTRING,@STRING, 
  " CASE WHEN FLOWS_SALESSTAGES_STATE_NAME IN ('quoteRequested', 'quoteCompleted','quoteDelivered', 'poReceived', 'customerCommitment','closedSale') ",
  " AND R.SOURCEKEY IS NULL AND R.RELNAME='primaryQuote' THEN 'M.101' ELSE 'NA' END as 'primaryQuote',",
  " CASE WHEN  FLOWS_SALESSTAGES_STATE_NAME IN ('poReceived','customerCommitment','closedSale')", 
  " AND R.SOURCEKEY IS NULL  AND R.RELNAME='booking' THEN 'M.101' ELSE 'NA' END  'booking' ,",
  "( SELECT  'M.102' FROM ",TENANT,".APP_QUOTES WHERE _ID<>P.DESTKEY )as 'quote',",
  " CASE WHEN P.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'quotem101',",
  "( SELECT  'M.102' FROM ",TENANT,".APP_CONTACTS WHERE _ID<>C.DESTKEY )as 'customer',",
  " CASE WHEN C.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'customerm101',",
  " CASE WHEN CA.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'basequotem101', _ID ",
  " FROM ",TENANT, ".APP_OPPORTUNITIES A ",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS P ON P.SOURCEKEY=A._ID AND P.SOURCETABLE='APP_ASSETS' AND P.RELNAME='product'",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS C ON C.SOURCEKEY=A._ID AND C.SOURCETABLE='APP_ASSETS' AND C.RELNAME='customer'",
  " LEFT JOIN ",TENANT,".RELATIONSHIPS CA ON CA.SOURCEKEY=A._ID AND CA.SOURCETABLE='APP_ASSETS' AND CA.RELNAME='covered'",
  " LEFT JOIN ",TENANT, ".RELATIONSHIPS R ON R.SOURCEKEY=A._ID )"
      
      );

    PREPARE DV_STMT3 FROM  @ASSETSTRING;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3;

  SET @EXSTRING=" INSERT INTO DQ_METRICS (TENANT,`OBJECT TYPE`,ATTRIBUTE,`RULE #`,`TOTAL # RECORDS`,`FAILED # RECORDS`,`FAILURE LEVEL`,DATETIME) ";
  drop table if exists SchemaColumn ;
  Create table  SchemaColumn as (SELECT Ordinal_position,column_name FROM information_schema.columns where table_schema='Obsidian' and table_name='OpportunityValidation_Dyn');
  delete from SchemaColumn where column_name not like '%.%';
  
  SET @COLCOUNT = (select count(1) from SchemaColumn);
  SET @STR1= " SELECT ";
  SET @STR2= " 'Opportunity' ";
  SET @STR3= " ( SELECT COUNT(";
  SET @STR4= " ) FROM OpportunityValidation_Dyn where ";
  SET @STR5= " ='";
  SET @STR6= "'), ";
  SET @STR7= "',NOW()";
  SET @STR8= "  UNION ALL ";
  
  LOOPIN: WHILE @COLCOUNT >0 DO
    SET @COLUMN=(SELECT column_name FROM SchemaColumn WHERE Ordinal_position=@COLCOUNT );
    
    IF @COLUMN<>""
    THEN 
    SET @ATTRIBUTE=(SELECT CASE WHEN INSTR(column_name,'.')>1 THEN SUBSTRING( column_name,1,INSTR(column_name,'.')-2) ELSE column_name END FROM SchemaColumn WHERE Ordinal_position= @COLCOUNT);
    SET @RULE=(SELECT  CASE WHEN INSTR(column_name,'.')>1 THEN SUBSTRING( column_name,INSTR(column_name,'.')-1,length(column_name)) ELSE column_name END FROM SchemaColumn WHERE Ordinal_position=@COLCOUNT);
    SET @LVL =(SELECT Lvl FROM ValidationConfiguration WHERE ColumnName=@ATTRIBUTE AND RuleName=@RULE AND EntityName='Opportunity');  
    SET @DQSTR=CONCAT(IFNULL(@DQSTR,''),@STR1,"'",TENANT,"'",",",@STR2,",'",@ATTRIBUTE,"','",ifnull(@RULE,''),"',",@ASSTCOUNT,",",@STR3,"`",@COLUMN,"`",@STR4,"`",@COLUMN,"`",@STR5,@RULE,@STR6,"'",@LVL,@STR7,@STR8  );
  
    SET @COLCOUNT =@COLCOUNT -1;
  
    END IF;
  END WHILE ;

    SET @EXCMD= CONCAT( @EXSTRING,@DQSTR,
    " SELECT '", TENANT,"','Opportunity','quote','M.102',",@ASSTCOUNT,",( SELECT COUNT(`quote`) FROM OpportunityValidation_Dyn where `quote`='M.102') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Opportunity','quote','M.101',",@ASSTCOUNT,",( SELECT COUNT(quotem101) FROM OpportunityValidation_Dyn where quotem101='M.101') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Opportunity','customer','M.102',",@ASSTCOUNT,",( SELECT COUNT(customer) FROM OpportunityValidation_Dyn where customer='M.102') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Opportunity','customer','M.101',",@ASSTCOUNT,",( SELECT COUNT(customerm101) FROM OpportunityValidation_Dyn where customerm101='M.101') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Opportunity','basequote','M.101',",@ASSTCOUNT,",( SELECT COUNT(basequotem101) FROM OpportunityValidation_Dyn where basequotem101='M.101') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Opportunity','booking','M.101',",@ASSTCOUNT,",( SELECT COUNT(booking) FROM OpportunityValidation_Dyn where booking='M.101') ,'ERROR',NOW()",
    " UNION ALL" ,
    " SELECT '", TENANT,"','Opportunity','primaryquote','M.101',",@ASSTCOUNT,",( SELECT COUNT(primaryquote) FROM OpportunityValidation_Dyn where primaryquote='M.101') ,'ERROR',NOW();"
  );
  

  PREPARE DV_STMT3 FROM  @EXCMD;
  EXECUTE DV_STMT3;
  DEALLOCATE PREPARE DV_STMT3;
  
  SET @DQSTR="";
  SET @STRING="";
  SET @ASSETSTRING="";
   
  
END;

-------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS Obsidian.spAssetValidation_Casedyn;
CREATE PROCEDURE Obsidian.`spAssetValidation_Casedyn`(IN TENANT varchar(40),IN ENTITY VARCHAR(25))
BEGIN
IF ENTITY='SERVICE_ASSETS'
THEN BEGIN
  SET @DQSTR="";
  SET @STRING="";
  SET @ASSETSTRING="";
  
SET @COUNT=CONCAT(
    " SELECT COUNT(1) INTO @ASSTCOUNT FROM ",TENANT,".SERVICE_ASSETS ; " );
    
    PREPARE DV_STMT3 FROM @COUNT;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3; 

drop table if exists AssetValidation_Dyn;
SET @CONSTRING=CONCAT(" CREATE TABLE AssetValidation_Dyn AS ( SELECT DISTINCT  ");
SET @CONSTRING4=" CASE WHEN " ;
SET @CONSTRING1= " THEN ";
SET @CONSTRING2= " WHEN ";
SET @CONSTRING3= " ELSE 'NA' END AS ";

SET @COLUMNCOUNT =(SELECT COUNT(1) FROM ValidationConfiguration WHERE EntityName='Service_Assets');
  OUTERLOOP: WHILE @COLUMNCOUNT>0 DO
 
  SET @RULEDES= (SELECT RuleDescription FROM ValidationConfiguration WHERE ID=@COLUMNCOUNT AND EntityName='Service_Assets');
  SET @RULENAME= (SELECT RuleName FROM ValidationConfiguration WHERE ID=@COLUMNCOUNT AND EntityName='Service_Assets');
  SET @COLNAME =  (SELECT ColumnName FROM ValidationConfiguration WHERE ID=@COLUMNCOUNT AND EntityName='Service_Assets');
  SET @STRING=CONCAT (ifnull(@STRING,''),@CONSTRING4,@RULEDES,@CONSTRING1,"'",@RULENAME,"'",@CONSTRING3,"'",@COLNAME,@RULENAME,"'",",");
  
  SET @COLUMNCOUNT=@COLUMNCOUNT-1;
  
  END WHILE;
 
  SET @ASSETSTRING= CONCAT(@CONSTRING,@STRING, 
       "( SELECT  'M.102' FROM ",TENANT,".APP_PRODUCTS WHERE _ID<>P.DESTKEY )as 'product',",
 " CASE WHEN P.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'productm101',",
  "( SELECT  'M.102' FROM ",TENANT,".APP_CONTACTS WHERE _ID<>C.DESTKEY )as 'customer',",
 " CASE WHEN C.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'customerm101',",
  "( SELECT  'M.102' FROM ",TENANT,".COVERED_ASSETS WHERE _ID<>CA.DESTKEY )as 'coveredasset',",
 " CASE WHEN CA.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'coveredassetm101',_ID, ",
 "  (SELECT 'MISC101' FROM ",TENANT,".APP_ASSETS AA LEFT JOIN ",TENANT,".RELATIONSHIPS B ON AA._ID=B.DESTKEY AND B.RELNAME='predecessor' ", 
      " WHERE AA.OPPORTUNITYGENERATED=1 AND AA._ID IS NULL AND A._ID=AA._ID) AS 'opportunityGenerated'"
" FROM ",TENANT, ".SERVICE_ASSETS A ",
" LEFT JOIN ",TENANT,".RELATIONSHIPS P ON P.SOURCEKEY=A._ID AND P.SOURCETABLE='APP_ASSETS' AND P.RELNAME='product'",
" LEFT JOIN ",TENANT,".RELATIONSHIPS C ON C.SOURCEKEY=A._ID AND C.SOURCETABLE='APP_ASSETS' AND C.RELNAME='customer'",
" LEFT JOIN ",TENANT,".RELATIONSHIPS CA ON CA.SOURCEKEY=A._ID AND CA.SOURCETABLE='APP_ASSETS' AND CA.RELNAME='covered'",
" LEFT JOIN ",TENANT,".RELATIONSHIPS B ON A._ID=B.DESTKEY AND B.RELNAME='predecessor')"
      
      );

  -- SELECT @ASSETSTRING;
    PREPARE DV_STMT3 FROM  @ASSETSTRING;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3;
    
  SET @EXSTRING=" INSERT INTO DQ_METRICS (TENANT,`OBJECT TYPE`,ATTRIBUTE,`RULE #`,`TOTAL # RECORDS`,`FAILED # RECORDS`,`FAILURE LEVEL`,DATETIME) ";
  drop table if exists SchemaColumn ;
  Create table  SchemaColumn as (SELECT Ordinal_position,column_name FROM information_schema.columns where table_schema='Obsidian' and table_name='AssetValidation_Dyn');
  delete from SchemaColumn where column_name not like '%.%';
  
  SET @COLCOUNT = (select count(1) from SchemaColumn);
  SET @STR1= " SELECT ";
  SET @STR2= " 'Service_Assets' ";
  SET @STR3= " ( SELECT COUNT(";
  SET @STR4= " ) FROM AssetValidation_Dyn where ";
  SET @STR5= " ='";
  SET @STR6= "'), ";
  SET @STR7= "',NOW()";
  SET @STR8= "  UNION ALL ";
  
  LOOPIN: WHILE @COLCOUNT >0 DO
    SET @COLUMN=(SELECT column_name FROM SchemaColumn WHERE Ordinal_position=@COLCOUNT );
    
    IF @COLUMN<>""
    THEN 
    SET @ATTRIBUTE=(SELECT CASE WHEN INSTR(column_name,'.')>1 THEN SUBSTRING( column_name,1,INSTR(column_name,'.')-2) ELSE column_name END FROM SchemaColumn WHERE Ordinal_position= @COLCOUNT);
    SET @RULE=(SELECT  CASE WHEN INSTR(column_name,'.')>1 THEN SUBSTRING( column_name,INSTR(column_name,'.')-1,length(column_name)) ELSE column_name END FROM SchemaColumn WHERE Ordinal_position=@COLCOUNT);
    SET @LVL =(SELECT Lvl FROM ValidationConfiguration WHERE ColumnName=@ATTRIBUTE AND RuleName=@RULE AND EntityName='Service_Assets');  
    SET @DQSTR=CONCAT(IFNULL(@DQSTR,''),@STR1,"'",TENANT,"'",",",@STR2,",'",@ATTRIBUTE,"','",ifnull(@RULE,''),"',",@ASSTCOUNT,",",@STR3,"`",@COLUMN,"`",@STR4,"`",@COLUMN,"`",@STR5,@RULE,@STR6,"'",@LVL,@STR7,@STR8  );
  
    SET @COLCOUNT =@COLCOUNT -1;
  
    END IF;
  END WHILE ;
   
      
    SET @EXCMD= CONCAT( @EXSTRING,@DQSTR,

    " SELECT '", TENANT,"', 'Service_Assets','Customer','M.102',",@ASSTCOUNT,",( SELECT COUNT(Customer) FROM AssetValidation_Dyn where Customer='M.102') ,'ERROR',NOW()",
     " UNION ALL" ,
    " SELECT '", TENANT,"', 'Service_Assets','Customer','M.101',",@ASSTCOUNT,",( SELECT COUNT(Customerm101) FROM AssetValidation_Dyn where Customerm101='M.101') ,'ERROR',NOW()",
     " UNION ALL" ,
    " SELECT '", TENANT,"', 'Service_Assets','Product','M.102',",@ASSTCOUNT,",( SELECT COUNT(Product) FROM AssetValidation_Dyn where Product='M.102') ,'ERROR',NOW()",
     " UNION ALL" ,
    " SELECT '", TENANT,"', 'Service_Assets','Product','M.101',",@ASSTCOUNT,",( SELECT COUNT(Productm101) FROM AssetValidation_Dyn where Productm101='M.101') ,'ERROR',NOW()",
     " UNION ALL" ,
   " SELECT '", TENANT,"', 'Service_Assets','Covered','M.102',",@ASSTCOUNT,",( SELECT COUNT(coveredasset) FROM AssetValidation_Dyn where coveredasset='M.102') ,'DEBUG',NOW()",
      " UNION ALL" ,
    " SELECT '", TENANT,"', 'Service_Assets','Covered','M.101',",@ASSTCOUNT,",( SELECT COUNT(coveredassetm101) FROM AssetValidation_Dyn where coveredassetm101='M.101') ,'DEBUG',NOW()",
      " UNION ALL" ,
    " SELECT '", TENANT,"', 'Service_Assets','opportunityGenerated','MISC101',",@ASSTCOUNT,",( SELECT COUNT(opportunityGenerated) FROM AssetValidation_Dyn where opportunityGenerated='MISC101') ,'ERROR',NOW();"
  );
  
  -- SELECT @EXCMD;
  
  PREPARE DV_STMT3 FROM  @EXCMD;
  EXECUTE DV_STMT3;
  DEALLOCATE PREPARE DV_STMT3;
  
  SET @DQSTR="";
  SET @STRING="";
  SET @ASSETSTRING="";
  END;
  END IF;
  
    IF ENTITY='COVERED_ASSETS'
    THEN BEGIN
    SET @COUNT=CONCAT(
        " SELECT COUNT(1) INTO @ASSTCOUNT FROM ",TENANT,".COVERED_ASSETS ; " );
        
        PREPARE DV_STMT3 FROM @COUNT;
        EXECUTE DV_STMT3;
        DEALLOCATE PREPARE DV_STMT3;

    DROP TABLE IF EXISTS AssetValidation;
    SET @CMD = CONCAT(
    " CREATE TABLE AssetValidation AS (",
     " SELECT CASE WHEN SYSTEMPROPERTIES_QRANK <> 4 THEN 'Q.101'ELSE 'NA'END 'SYSTEMPROPERTIES_QRANK',",
     "( SELECT  'M.102' FROM ",TENANT,".APP_PRODUCTS WHERE _ID<>P.DESTKEY )as 'product',",
     " CASE WHEN P.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'productm101'",
     
    " FROM ",TENANT, ".COVERED_ASSETS A ",
    " LEFT JOIN ",TENANT,".RELATIONSHIPS P ON P.SOURCEKEY=A._ID AND P.SOURCETABLE='APP_ASSETS' AND P.RELNAME='product');"
    );
     PREPARE DV_STMT3 FROM @CMD;
        EXECUTE DV_STMT3;
        DEALLOCATE PREPARE DV_STMT3;
        
        INSERT INTO DQ_METRICS (TENANT,`OBJECT TYPE`,ATTRIBUTE,`RULE #`,`TOTAL # RECORDS`,`FAILED # RECORDS`,`FAILURE LEVEL`,DATETIME)
         select TENANT,'Covered_Assets','Systemproperties.QRank','Q.101',@ASSTCOUNT,( SELECT COUNT(SYSTEMPROPERTIES_QRANK) FROM AssetValidation where SYSTEMPROPERTIES_QRANK='Q.101') ,'ERROR',NOW()
         union all
         SELECT TENANT,'Covered_Assets','Product','M.102',@ASSTCOUNT,( SELECT COUNT(Product) FROM AssetValidation where Product='M.102') ,'ERROR',NOW()
         UNION ALL
        SELECT TENANT,'Covered_Assets','Product','M.101',@ASSTCOUNT,( SELECT COUNT(Productm101) FROM AssetValidation where Productm101='M.101') ,'ERROR',NOW();
        
  END;
  END IF;
  
END;

-----------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS Obsidian.spCommaSplittedValues;
CREATE PROCEDURE Obsidian.`spCommaSplittedValues`(IN TENANT varchar(40))
BEGIN 
DROP TEMPORARY TABLE IF EXISTS Temp_ServiceAsset;
     SET @CMD=CONCAT(
     " CREATE TEMPORARY TABLE Temp_ServiceAsset AS  " ,
     " SELECT _ID,EXTERNALIDS_ID,EXTERNALIDS_SCHEMEID_NAME FROM ",TENANT,".SERVICE_ASSETS;"
     );
    PREPARE DV_STMT3 FROM @CMD;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3;
    
  BEGIN
    DECLARE id VARCHAR(255) DEFAULT 0;
    DECLARE value VARCHAR(255);
     DECLARE sschema VARCHAR(255);
    DECLARE occurance INT DEFAULT 0;
    DECLARE i INT DEFAULT 0;
    DECLARE splitted_value VARCHAR(255);
    DECLARE splitted_schema VARCHAR(255);
    DECLARE done INT DEFAULT 0;
    DECLARE cur1 CURSOR FOR SELECT _ID,EXTERNALIDS_ID,EXTERNALIDS_SCHEMEID_NAME
                                         FROM Temp_ServiceAsset;
                                         
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    DROP TABLE IF EXISTS ExternalIdSplitted;
    CREATE TABLE ExternalIdSplitted(
    `id` VARCHAR(255) NOT NULL,
    `value` VARCHAR(255) NOT NULL,
    `sschema` VARCHAR(255) NOT NULL
    ) ;

    OPEN cur1;
      read_loop: LOOP
        FETCH cur1 INTO id, value,sschema;
        IF done THEN
          LEAVE read_loop;
        END IF;

        SET occurance = (SELECT LENGTH(value)
                                 - LENGTH(REPLACE(value, ',', ''))
                                 +1);
        SET i=1;
        WHILE i <= occurance DO
          SET splitted_value =
          (SELECT REPLACE(SUBSTRING(SUBSTRING_INDEX(value, ',', i),
          LENGTH(SUBSTRING_INDEX(value, ',', i - 1)) + 1), ',', ''));
          
          SET splitted_schema =
          (SELECT REPLACE(SUBSTRING(SUBSTRING_INDEX(sschema, ',', i),
          LENGTH(SUBSTRING_INDEX(sschema, ',', i - 1)) + 1), ',', ''));

          INSERT INTO ExternalIdSplitted VALUES (id, splitted_value,splitted_schema);
          SET i = i + 1;

        END WHILE;
      END LOOP;
    CLOSE cur1;
  END;
  END;



-----------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS Obsidian.spAssetValidation_Case;
CREATE PROCEDURE Obsidian.`spAssetValidation_Case`(IN TENANT varchar(40),IN ENTITY VARCHAR(25))
BEGIN
IF ENTITY='SERVICE_ASSETS'
THEN BEGIN
SET @COUNT=CONCAT(
    " SELECT COUNT(1) INTO @ASSTCOUNT FROM ",TENANT,".SERVICE_ASSETS ; " );
    
    PREPARE DV_STMT3 FROM @COUNT;
    EXECUTE DV_STMT3;

DROP TABLE IF EXISTS AssetValidation;
SET @CMD = CONCAT(
" CREATE TABLE AssetValidation AS (",

" SELECT distinct CASE WHEN amount_amount =0.00 THEN  'C.101' WHEN amount_amount < 0.00 THEN 'C.102'ELSE 'NA'END 'AMOUNT' ,",

" CASE WHEN AMOUNT_CODE_NAME IS NULL THEN 'C.103'ELSE 'NA'END 'AMOUNT_CODE_NAME',",

" CASE WHEN AMOUNT_NORMALIZEDAMOUNT_AMOUNT =0.00 THEN 'C.105' ELSE 'NA'END 'AMOUNT_NORMALIZEDAMOUNT_AMOUNT',",

" CASE WHEN AMOUNT_NORMALIZEDAMOUNT_CODE_NAME IS NULL THEN 'C.106' ELSE 'NA'END 'AMOUNT_NORMALIZEDAMOUNT_CODE_NAME',",

" CASE WHEN AMOUNT_NORMALIZEDAMOUNT_AMOUNT=amount_amount AND AMOUNT_CODE_NAME<>'usd' THEN 'C.107' ELSE 'NA'END 'AMOUNT_NORMALIZEDAMOUNT_AMOUNT&AMOUNT_CODE_NAME',",

 " CASE WHEN SYSTEMPROPERTIES_QRANK <> 4 THEN 'Q.101'ELSE 'NA'END 'SYSTEMPROPERTIES_QRANK',",

 " CASE WHEN STARTDATE ='0000-00-00 00:00:00' THEN 'D.101' WHEN STARTDATE <'2000-01-01' THEN 'D.102' WHEN STARTDATE >'2049-12-31' THEN 'D.103' ELSE 'NA'END 'STARTDATE',",

 " CASE WHEN ENDDATE ='0000-00-00 00:00:00' THEN 'D.101' WHEN ENDDATE <'2000-01-01' THEN 'D.102' WHEN ENDDATE >'2049-12-31' THEN 'D.103' ELSE 'NA'END 'ENDDATE',",

" CASE WHEN EXTENSIONS_MASTER_COUNTRY_VALUE_NAME IS NULL THEN 'L.101' ELSE 'NA'END 'EXTENSIONS_MASTER_COUNTRY_VALUE_NAME',",

" CASE WHEN EXTENSIONS_MASTER_COUNTRY_VALUE_DISPLAYNAME IS NULL OR EXTENSIONS_MASTER_COUNTRY_VALUE_KEY IS NULL THEN 'L.102' ELSE 'NA'END 'EXTENSIONS_MASTER_COUNTRY_VALUE_DISPLAYNAME',",

" CASE WHEN EXTENSIONS_MASTER_COUNTRY_VALUE_KEY IS NULL THEN 'L.103' ELSE 'NA'END 'EXTENSIONS_MASTER_COUNTRY_VALUE_KEY',",

" CASE WHEN EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_NAME IS NULL THEN 'L.101' ELSE 'NA'END 'EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_NAME',",

" CASE WHEN EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_DISPLAYNAME IS NULL OR EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_KEY IS NULL THEN 'L.102' ELSE 'NA'END 'EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_DISPLAYNAME',",

" CASE WHEN EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_KEY IS NULL THEN 'L.103' ELSE 'NA'END 'EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_KEY',",

" CASE WHEN EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_NAME IS NULL THEN 'L.101' ELSE 'NA'END 'EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_NAME',",

" CASE WHEN EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_DISPLAYNAME IS NULL OR EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_KEY IS NULL THEN 'L.102' ELSE 'NA'END 'EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_DISPLAYNAME',",

" CASE WHEN EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_KEY IS NULL THEN 'L.103' ELSE 'NA'END 'EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_KEY',",
" CASE WHEN EXTENSIONS_MASTER_CLIENTREGION_VALUE_NAME IS NULL THEN 'L.101' ELSE 'NA'END 'EXTENSIONS_MASTER_CLIENTREGION_VALUE_NAME',",
" CASE WHEN EXTENSIONS_MASTER_CLIENTREGION_VALUE_DISPLAYNAME IS NULL OR EXTENSIONS_MASTER_CLIENTREGION_VALUE_KEY IS NULL THEN 'L.102' ELSE 'NA'END 'EXTENSIONS_MASTER_CLIENTREGION_VALUE_DISPLAYNAME',",
" CASE WHEN EXTENSIONS_MASTER_CLIENTREGION_VALUE_KEY IS NULL THEN 'L.103' ELSE 'NA'END 'EXTENSIONS_MASTER_CLIENTREGION_VALUE_KEY',",
 "( SELECT  'M.102' FROM ",TENANT,".APP_PRODUCTS WHERE _ID<>P.DESTKEY )as 'product',",
 " CASE WHEN P.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'productm101',",
  "( SELECT  'M.102' FROM ",TENANT,".APP_CONTACTS WHERE _ID<>C.DESTKEY )as 'customer',",
 " CASE WHEN C.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'customerm101',",
  "( SELECT  'M.102' FROM ",TENANT,".COVERED_ASSETS WHERE _ID<>CA.DESTKEY )as 'coveredasset',",
 " CASE WHEN CA.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'coveredassetm101',_ID, ",
 "  (SELECT 'MISC101' FROM ",TENANT,".APP_ASSETS AA LEFT JOIN ",TENANT,".RELATIONSHIPS B ON AA._ID=B.DESTKEY AND B.RELNAME='predecessor' ", 
      " WHERE AA.OPPORTUNITYGENERATED=1 AND AA._ID IS NULL AND A._ID=AA._ID) AS 'opportunityGenerated' ,",
  " ( SELECT DISTINCT CASE WHEN sschema='' THEN 'X.101' ELSE 'NA' END 'EXTERNALIDX.101' FROM ExternalIdSplitted WHERE id=A._ID ) AS 'EXTERNALIDX.101' ,",
  " ( SELECT 'X.102' FROM ExternalIdSplitted WHERE id=A._ID GROUP BY id,value HAVING count(value)>1 ) AS 'EXTERNALIDX.102' ,",
  " ( SELECT DISTINCT CASE WHEN sschema='batchload' THEN 'X.103' ELSE 'NA' END 'EXTERNALIDX.103' FROM ExternalIdSplitted WHERE id=A._ID ) AS 'EXTERNALIDX.103' ",
" FROM ",TENANT, ".SERVICE_ASSETS A ",
" LEFT JOIN ",TENANT,".RELATIONSHIPS P ON P.SOURCEKEY=A._ID AND P.SOURCETABLE='APP_ASSETS' AND P.RELNAME='product'",
" LEFT JOIN ",TENANT,".RELATIONSHIPS C ON C.SOURCEKEY=A._ID AND C.SOURCETABLE='APP_ASSETS' AND C.RELNAME='customer'",
" LEFT JOIN ",TENANT,".RELATIONSHIPS CA ON CA.SOURCEKEY=A._ID AND CA.SOURCETABLE='APP_ASSETS' AND CA.RELNAME='covered'",
" LEFT JOIN ",TENANT,".RELATIONSHIPS B ON A._ID=B.DESTKEY AND B.RELNAME='predecessor')"
);
 PREPARE DV_STMT3 FROM @CMD;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3;

INSERT INTO DQ_METRICS (TENANT,`OBJECT TYPE`,ATTRIBUTE,`RULE #`,`TOTAL # RECORDS`,`FAILED # RECORDS`,`FAILURE LEVEL`,DATETIME)
    select TENANT,'Service_Assets','Amount_Amount','C.101',@ASSTCOUNT,( SELECT COUNT(AMOUNT) FROM AssetValidation where AMOUNT='C.101') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Service_Assets','Amount_Amount','C.102',@ASSTCOUNT,( SELECT COUNT(AMOUNT) FROM AssetValidation where AMOUNT='C.102') ,'ERROR',NOW()
    UNION ALL 
    select TENANT,'Service_Assets','Systemproperties.QRank','Q.101',@ASSTCOUNT,( SELECT COUNT(SYSTEMPROPERTIES_QRANK) FROM AssetValidation where SYSTEMPROPERTIES_QRANK='Q.101') ,'WARN',NOW()
    UNION ALL 
    select TENANT,'Service_Assets','StartDate','D.101',@ASSTCOUNT,( SELECT COUNT(STARTDATE) FROM AssetValidation where STARTDATE='D.101') ,'WARN',NOW()
    UNION ALL 
    select TENANT,'Service_Assets','StartDate','D.102',@ASSTCOUNT,( SELECT COUNT(STARTDATE) FROM AssetValidation where STARTDATE='D.102') ,'WARN',NOW()
    UNION ALL 
    select TENANT,'Service_Assets','StartDate','D.103',@ASSTCOUNT,( SELECT COUNT(STARTDATE) FROM AssetValidation where STARTDATE='D.103') ,'WARN',NOW()
    UNION ALL 
    select TENANT,'Service_Assets','EndDate','D.101',@ASSTCOUNT,( SELECT COUNT(ENDDATE) FROM AssetValidation where ENDDATE='D.101') ,'ERROR',NOW()
    UNION ALL 
    select TENANT,'Service_Assets','EndDate','D.102',@ASSTCOUNT,( SELECT COUNT(ENDDATE) FROM AssetValidation where ENDDATE='D.102') ,'ERROR',NOW()
    UNION ALL 
    select TENANT,'Service_Assets','EndDate','D.103',@ASSTCOUNT,( SELECT COUNT(ENDDATE) FROM AssetValidation where ENDDATE='D.103') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Service_Assets','AmountCode','C.103',@ASSTCOUNT,( SELECT COUNT(AMOUNT_CODE_NAME) FROM AssetValidation where AMOUNT_CODE_NAME='C.103') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Service_Assets','NormalizedAmount','C.105',@ASSTCOUNT,( SELECT COUNT(AMOUNT_NORMALIZEDAMOUNT_AMOUNT) FROM AssetValidation where AMOUNT_NORMALIZEDAMOUNT_AMOUNT='C.105') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Service_Assets','NormalizedAmountCode','C.106',@ASSTCOUNT,( SELECT COUNT(AMOUNT_NORMALIZEDAMOUNT_CODE_NAME) FROM AssetValidation where AMOUNT_NORMALIZEDAMOUNT_CODE_NAME='C.106') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Service_Assets','NormalizedAmountCode','C.108',@ASSTCOUNT,( SELECT COUNT(`AMOUNT_NORMALIZEDAMOUNT_AMOUNT&AMOUNT_CODE_NAME`) FROM AssetValidation where `AMOUNT_NORMALIZEDAMOUNT_AMOUNT&AMOUNT_CODE_NAME`='C.108') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Service_Assets','COUNTRY Name','L.101',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_COUNTRY_VALUE_NAME) FROM AssetValidation where EXTENSIONS_MASTER_COUNTRY_VALUE_NAME='L.101') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','COUNTRY DisplayName','L.102',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_COUNTRY_VALUE_DISPLAYNAME) FROM AssetValidation where EXTENSIONS_MASTER_COUNTRY_VALUE_DISPLAYNAME='L.102') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','COUNTRY Key','L.103',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_COUNTRY_VALUE_KEY) FROM AssetValidation where EXTENSIONS_MASTER_COUNTRY_VALUE_KEY='L.103') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','CLIENTTERRITORY Name','L.101',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_NAME) FROM AssetValidation where EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_NAME='L.101') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','CLIENTTERRITORY DisplayName','L.102',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_DISPLAYNAME) FROM AssetValidation where EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_DISPLAYNAME='L.102') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','CLIENTTERRITORY Key','L.103',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_KEY) FROM AssetValidation where EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_KEY='L.103') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','CLIENTREGION Name','L.101',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_NAME) FROM AssetValidation where EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_NAME='L.101') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','CLIENTREGION DisplayName','L.102',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_DISPLAYNAME) FROM AssetValidation where EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_DISPLAYNAME='L.102') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','CLIENTREGION Key','L.103',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_KEY) FROM AssetValidation where EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_KEY='L.103') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','CLIENTTHEATRE Name','L.101',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_CLIENTREGION_VALUE_NAME) FROM AssetValidation where EXTENSIONS_MASTER_CLIENTREGION_VALUE_NAME='L.101') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','CLIENTTHEATRE DisplayName','L.102',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_CLIENTREGION_VALUE_DISPLAYNAME) FROM AssetValidation where EXTENSIONS_MASTER_CLIENTREGION_VALUE_DISPLAYNAME='L.102') ,'DEBUG',NOW()
    UNION ALL
    select TENANT,'Service_Assets','CLIENTTHEATRE Key','L.103',@ASSTCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_CLIENTREGION_VALUE_KEY) FROM AssetValidation where EXTENSIONS_MASTER_CLIENTREGION_VALUE_KEY='L.103') ,'DEBUG',NOW()
    UNION ALL
    SELECT TENANT,'Service_Assets','Customer','M.102',@ASSTCOUNT,( SELECT COUNT(Customer) FROM AssetValidation where Customer='M.102') ,'ERROR',NOW()
     UNION ALL
    SELECT TENANT,'Service_Assets','Customer','M.101',@ASSTCOUNT,( SELECT COUNT(Customerm101) FROM AssetValidation where Customerm101='M.101') ,'ERROR',NOW()
     UNION ALL
    SELECT TENANT,'Service_Assets','Product','M.102',@ASSTCOUNT,( SELECT COUNT(Product) FROM AssetValidation where Product='M.102') ,'ERROR',NOW()
     UNION ALL
    SELECT TENANT,'Service_Assets','Product','M.101',@ASSTCOUNT,( SELECT COUNT(Productm101) FROM AssetValidation where Productm101='M.101') ,'ERROR',NOW()
     UNION ALL
    SELECT TENANT,'Service_Assets','Covered','M.102',@ASSTCOUNT,( SELECT COUNT(coveredasset) FROM AssetValidation where coveredasset='M.102') ,'DEBUG',NOW()
     UNION ALL
    SELECT TENANT,'Service_Assets','Covered','M.101',@ASSTCOUNT,( SELECT COUNT(coveredassetm101) FROM AssetValidation where coveredassetm101='M.101') ,'DEBUG',NOW()
     UNION ALL
    SELECT TENANT,'Service_Assets','opportunityGenerated','MISC101',@ASSTCOUNT,( SELECT COUNT(opportunityGenerated) FROM AssetValidation where opportunityGenerated='MISC101') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Service_Assets','ExternalId','X.101',@ASSTCOUNT,( SELECT COUNT(`EXTERNALIDX.101`) FROM AssetValidation where `EXTERNALIDX.101`='X.101') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Service_Assets','ExternalId','X.102',@ASSTCOUNT,( SELECT COUNT(`EXTERNALIDX.102`) FROM AssetValidation where `EXTERNALIDX.102`='X.102') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Service_Assets','ExternalId','X.103',@ASSTCOUNT,( SELECT COUNT(`EXTERNALIDX.103`) FROM AssetValidation where `EXTERNALIDX.103`='X.103') ,'ERROR',NOW();
  END;
  END IF;

    IF ENTITY='COVERED_ASSETS'
    THEN BEGIN
    SET @COUNT=CONCAT(
        " SELECT COUNT(1) INTO @ASSTCOUNT FROM ",TENANT,".COVERED_ASSETS ; " );
        
        PREPARE DV_STMT3 FROM @COUNT;
        EXECUTE DV_STMT3;
        DEALLOCATE PREPARE DV_STMT3;

    DROP TABLE IF EXISTS AssetValidation;
    SET @CMD = CONCAT(
    " CREATE TABLE AssetValidation AS (",
     " SELECT CASE WHEN SYSTEMPROPERTIES_QRANK <> 4 THEN 'Q.101'ELSE 'NA'END 'SYSTEMPROPERTIES_QRANK',",
     "( SELECT  'M.102' FROM ",TENANT,".APP_PRODUCTS WHERE _ID<>P.DESTKEY )as 'product',",
     " CASE WHEN P.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'productm101'",
     
    " FROM ",TENANT, ".COVERED_ASSETS A ",
    " LEFT JOIN ",TENANT,".RELATIONSHIPS P ON P.SOURCEKEY=A._ID AND P.SOURCETABLE='APP_ASSETS' AND P.RELNAME='product');"
    );
     PREPARE DV_STMT3 FROM @CMD;
        EXECUTE DV_STMT3;
        DEALLOCATE PREPARE DV_STMT3;
        
        INSERT INTO DQ_METRICS (TENANT,`OBJECT TYPE`,ATTRIBUTE,`RULE #`,`TOTAL # RECORDS`,`FAILED # RECORDS`,`FAILURE LEVEL`,DATETIME)
         select TENANT,'Covered_Assets','Systemproperties.QRank','Q.101',@ASSTCOUNT,( SELECT COUNT(SYSTEMPROPERTIES_QRANK) FROM AssetValidation where SYSTEMPROPERTIES_QRANK='Q.101') ,'ERROR',NOW()
         union all
         SELECT TENANT,'Covered_Assets','Product','M.102',@ASSTCOUNT,( SELECT COUNT(Product) FROM AssetValidation where Product='M.102') ,'ERROR',NOW()
         UNION ALL
        SELECT TENANT,'Covered_Assets','Product','M.101',@ASSTCOUNT,( SELECT COUNT(Productm101) FROM AssetValidation where Productm101='M.101') ,'ERROR',NOW();
        
  END;
  END IF;
  
END;

---------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS Obsidian.SP_OpportunityExceptionReport;
CREATE PROCEDURE Obsidian.`SP_OpportunityExceptionReport`(
    Tenant varchar(100) )
BEGIN 

SET @OPPCOUNT=CONCAT (
  "(SELECT COUNT(1) INTO @ResCount FROM ",TENANT,".APP_OPPORTUNITIES)");

  PREPARE DV_STMT8 FROM @OPPCOUNT;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
 SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME='closedsale'", 
" and AMOUNT_AMOUNT > 0.01 and AMOUNT_AMOUNT < 0.99");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE TARGETAMOUNT_AMOUNT > 0.01 AND TARGETAMOUNT_AMOUNT < 0.99");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;

 SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME='poReceived'", 
" and EXTENSIONS_TENANT_NEWPONUMBER_VALUE =''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME='poReceived'", 
" and EXTENSIONS_TENANT_NEWPONUMBER_VALUE =''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME='closedsale'", 
" and RESOLUTIONDATE=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE AMOUNT_AMOUNT < 0.00");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
    SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE TARGETAMOUNT_AMOUNT < 0.00");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME IN ('houseAccount','noService','open')",
" AND EXTENSIONS_TENANT_POLYCOMBOOKINGDATE_VALUE <> ''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
     SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE EXTENSIONS_MASTER_CLIENTTERRITORY_VALUE_DISPLAYNAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
  SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE AMOUNT_AMOUNT ='' AND FLOWS_SALESSTAGES_STATE_NAME='closedsale'");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
     SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE EXTENSIONS_MASTER_CLIENTTHEATRE_VALUE_DISPLAYNAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;

 SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE TARGETAMOUNT_AMOUNT =''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE EXTENSIONS_MASTER_CLIENTREGION_VALUE_DISPLAYNAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME='closedsale'", 
" and EXTENSIONS_TENANT_NEWPONUMBER_VALUE =''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
  SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE EXTENSIONS_MASTER_CLIENTREGION_VALUE_DISPLAYNAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE EXTENSIONS_MASTER_COUNTRY_VALUE_DISPLAYNAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
  SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE EXTENSIONS_MASTER_DIRECTCHANNEL_VALUE_DISPLAYNAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
    SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE EXTENSIONS_MASTER_BUSINESSLINE_VALUE_DISPLAYNAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
    SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".RELATIONSHIPS R ",
" LEFT JOIN ",TENANT ,".APP_CONTACTS C ON C._ID=R.DESTKEY WHERE R.relname='customer'");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME='closedsale'", 
" and EXTENSIONS_TENANT_POLYCOMBOOKINGDATE_VALUE =''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
  
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES", 
"  WHERE DATE(EXTENSIONS_MASTER_EARLIESTNEWSTARTDATE_VALUE) > DATE(EXTENSIONS_MASTER_LATESTNEWENDDATE_VALUE)");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
     SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME='closedsale'", 
" and EXTENSIONS_MASTER_EARLIESTNEWSTARTDATE_VALUE =''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
     SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME='closedsale'", 
" and EXTENSIONS_MASTER_LATESTNEWENDDATE_VALUE =''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
   SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME='closedsale'", 
" and COMMITLEVEL_NAME <> 'green'");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
    SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE FLOWS_SALESSTAGES_STATE_NAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
    SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE AMOUNT_AMOUNT <>'' and AMOUNT_CODE_NAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
    
    SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE  TARGETAMOUNT_CODE_NAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
    SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  FROM ",TENANT ,".APP_OPPORTUNITIES WHERE  TARGETAMOUNT_CODE_NAME=''");

  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
  
  SET @CMD = CONCAT(
 "INSERT INTO DQ_METRICS",
" SELECT '",TENANT ,"','Opporutnities','','','", @ResCount ,"',COUNT(1),'',NOW()  ",
" FROM ( select  @row_num := IF( @prev_value =concat_ws('',_ID),@row_num+1,1) AS RowNumber,",
"  @prev_value:= concat_ws('',_ID) as Prev FROM ",TENANT ,".APP_OPPORTUNITIES )A WHERE RowNumber >1 ");

  
  PREPARE DV_STMT8 FROM @CMD;
  EXECUTE DV_STMT8;
  DEALLOCATE PREPARE DV_STMT8;
end;
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS Obsidian.spOfferValidation_Case;
CREATE PROCEDURE Obsidian.`spOfferValidation_Case`(IN TENANT varchar(40))
BEGIN

SET @COUNT=CONCAT(
    " SELECT COUNT(1) INTO @OPPCOUNT FROM ",TENANT,".APP_OFFERS ; " );
    
    PREPARE DV_STMT3 FROM @COUNT;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3;

DROP TABLE IF EXISTS OfferValidation;
SET @CMD = CONCAT(
" CREATE TABLE OfferValidation AS (",

" SELECT distinct CASE WHEN amount_amount =0.00 THEN  'C.101' WHEN amount_amount < 0.00 THEN 'C.102'ELSE 'NA'END 'AMOUNT' ,",

" CASE WHEN AMOUNT_CODE_NAME IS NULL THEN 'C.103'ELSE 'NA'END 'AMOUNT_CODE_NAME',",

" CASE WHEN AMOUNT_NORMALIZEDAMOUNT_AMOUNT =0.00 THEN 'C.105' ELSE 'NA'END 'AMOUNT_NORMALIZEDAMOUNT_AMOUNT',",

" CASE WHEN AMOUNT_NORMALIZEDAMOUNT_CODE_NAME IS NULL THEN 'C.106' ELSE 'NA'END 'AMOUNT_NORMALIZEDAMOUNT_CODE_NAME',",

" CASE WHEN AMOUNT_NORMALIZEDAMOUNT_AMOUNT=amount_amount AND AMOUNT_CODE_NAME<>'usd' THEN 'C.107' ELSE 'NA'END 'AMOUNT_NORMALIZEDAMOUNT_AMOUNT&AMOUNT_CODE_NAME',",

" CASE WHEN TARGETAMOUNT_AMOUNT =0.00 THEN  'C.101' WHEN TARGETAMOUNT_AMOUNT < 0.00 THEN 'C.102'ELSE 'NA'END 'TARGETAMOUNT_AMOUNT',",

" CASE WHEN TARGETAMOUNT_CODE_NAME IS NULL THEN 'C.103'ELSE 'NA'END 'TARGETAMOUNT_CODE_NAME',",

" CASE WHEN TARGETAMOUNT_NORMALIZEDAMOUNT_AMOUNT =0.00 THEN 'C.105' ELSE 'NA'END 'TARGETAMOUNT_NORMALIZEDAMOUNT_AMOUNT',",

" CASE WHEN TARGETAMOUNT_NORMALIZEDAMOUNT_CODE_NAME IS NULL THEN 'C.106' ELSE 'NA'END 'TARGETAMOUNT_NORMALIZEDAMOUNT_CODE_NAME',",

" CASE WHEN TARGETAMOUNT_NORMALIZEDAMOUNT_AMOUNT=TARGETAMOUNT_AMOUNT AND TARGETAMOUNT_CODE_NAME<>'usd' THEN 'C.107' ELSE 'NA' END 'TARGETAMOUNT_NORMALIZEDAMOUNT_AMOUNT&TARGETAMOUNT_CODE_NAME',",


" CASE WHEN STARTDATE ='0000-00-00 00:00:00' THEN 'D.101' WHEN STARTDATE <'2000-01-01' THEN 'D.102' WHEN STARTDATE >'2049-12-31' THEN 'D.103' ELSE 'NA' END 'STARTDATE',",

" CASE WHEN ENDDATE ='0000-00-00 00:00:00' THEN 'D.101' WHEN ENDDATE <'2000-01-01' THEN 'D.102' WHEN ENDDATE >'2049-12-31' THEN 'D.103' ELSE 'NA' END 'ENDDATE',",

" CASE WHEN TARGETDATE ='0000-00-00 00:00:00' THEN 'D.101' WHEN TARGETDATE <'2000-01-01' THEN 'D.102' WHEN TARGETDATE >'2049-12-31' THEN 'D.103' ELSE 'NA' END 'TARGETDATE',",

" CASE WHEN EXTENSIONS_MASTER_BATCHTYPE_VALUE_NAME IS NULL THEN 'L.101' ELSE 'NA'END 'EXTENSIONS_MASTER_BATCHTYPE_VALUE_NAME',",

" CASE WHEN EXTENSIONS_MASTER_BATCHTYPE_VALUE_DISPLAYNAME IS NULL OR EXTENSIONS_MASTER_BATCHTYPE_VALUE_KEY IS NULL THEN 'L.102' ELSE 'NA' END 'EXTENSIONS_MASTER_BATCHTYPE_VALUE_DISPLAYNAME',",

" CASE WHEN EXTENSIONS_MASTER_BATCHTYPE_VALUE_KEY IS NULL THEN 'L.103' ELSE 'NA'END 'EXTENSIONS_MASTER_BATCHTYPE_VALUE_KEY' ,",

  "( SELECT  'M.102' FROM ",TENANT,".APP_QUOTES WHERE _ID<>Q.DESTKEY )as 'quote',",
 " CASE WHEN Q.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'quotem101',",
  "( SELECT  'M.102' FROM ",TENANT,".APP_CONTACTS WHERE _ID<>C.DESTKEY )as 'customer',",
 " CASE WHEN C.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'customerm101',",
 "( SELECT  'M.102' FROM ",TENANT,".APP_PRODUCTS WHERE _ID<>P.DESTKEY )as 'product',",
 " CASE WHEN P.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'productm101',",
 "( SELECT  'M.102' FROM ",TENANT,".SERVICE_ASSETS WHERE _ID<>PR.DESTKEY )as 'predecessor',",
 " CASE WHEN PR.SOURCEKEY IS NULL THEN 'M.101' ELSE 'NA' END as 'predecessorm101',",
 
  "( SELECT 'M.101' FROM  ",TENANT,".SERVICE_ASSETS A ",
    " LEFT JOIN ",TENANT,".RELATIONSHIPS R1 ON R1.DESTKEY=PR.DESTKEY AND R1.RELNAME='reseller' WHERE A._ID=PR.DESTKEY and R.DESTKEY IS NULL  ) as 'reseller',",
 
  "( SELECT 'M.101' FROM  ",TENANT,".SERVICE_ASSETS A ",
    " LEFT JOIN ",TENANT,".RELATIONSHIPS R1 ON R1.DESTKEY=PR.DESTKEY AND R1.RELNAME='distributor' WHERE A._ID=PR.DESTKEY and R.DESTKEY IS NULL  ) as 'distributor' ",

" FROM ",TENANT, ".APP_OFFERS A ",
" LEFT JOIN ",TENANT,".RELATIONSHIPS E on E.SOURCEKEY=A._ID",
" LEFT JOIN ",TENANT,".RELATIONSHIPS Q ON Q.SOURCEKEY=A._ID AND Q.SOURCETABLE='APP_ASSETS' AND Q.RELNAME='quote'",
" LEFT JOIN ",TENANT,".RELATIONSHIPS C ON C.SOURCEKEY=A._ID AND C.SOURCETABLE='APP_ASSETS' AND C.RELNAME='customer'",
" LEFT JOIN ",TENANT,".RELATIONSHIPS P ON P.SOURCEKEY=A._ID AND P.SOURCETABLE='APP_ASSETS' AND P.RELNAME='product'",
" LEFT JOIN ",TENANT,".RELATIONSHIPS PR ON PR.SOURCEKEY=A._ID AND PR.SOURCETABLE='APP_ASSETS' AND PR.RELNAME='predecessor');"
);
 PREPARE DV_STMT3 FROM @CMD;
    EXECUTE DV_STMT3;
    DEALLOCATE PREPARE DV_STMT3;

INSERT INTO DQ_METRICS (TENANT,`OBJECT TYPE`,ATTRIBUTE,`RULE #`,`TOTAL # RECORDS`,`FAILED # RECORDS`,`FAILURE LEVEL`,DATETIME)
    select TENANT,'Offer','Amount_Amount','C.101',@OPPCOUNT,( SELECT COUNT(AMOUNT) FROM OfferValidation where AMOUNT='C.101') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','Amount_Amount','C.102',@OPPCOUNT,( SELECT COUNT(AMOUNT) FROM OfferValidation where AMOUNT='C.102') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','AmountCode','C.103',@OPPCOUNT,( SELECT COUNT(AMOUNT_CODE_NAME) FROM OfferValidation where AMOUNT_CODE_NAME='C.103') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','NormalizedAmount','C.105',@OPPCOUNT,( SELECT COUNT(AMOUNT_NORMALIZEDAMOUNT_AMOUNT) FROM OfferValidation where AMOUNT_NORMALIZEDAMOUNT_AMOUNT='C.105') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','NormalizedAmountCode','C.106',@OPPCOUNT,( SELECT COUNT(AMOUNT_NORMALIZEDAMOUNT_CODE_NAME) FROM OfferValidation where AMOUNT_NORMALIZEDAMOUNT_CODE_NAME='C.106') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','NormalizedAmountCode','C.108',@OPPCOUNT,( SELECT COUNT(`AMOUNT_NORMALIZEDAMOUNT_AMOUNT&AMOUNT_CODE_NAME`) FROM OfferValidation where `AMOUNT_NORMALIZEDAMOUNT_AMOUNT&AMOUNT_CODE_NAME`='C.108') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','TargetAmount_Amount','C.101',@OPPCOUNT,( SELECT COUNT(TARGETAMOUNT_AMOUNT) FROM OfferValidation where TARGETAMOUNT_AMOUNT='C.101') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','TargetAmount_Amount','C.102',@OPPCOUNT,( SELECT COUNT(TARGETAMOUNT_AMOUNT) FROM OfferValidation where TARGETAMOUNT_AMOUNT='C.102') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','TargetAmountCode','C.103',@OPPCOUNT,( SELECT COUNT(TARGETAMOUNT_CODE_NAME) FROM OfferValidation where TARGETAMOUNT_CODE_NAME='C.103') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','TargetNormalizedAmount','C.105',@OPPCOUNT,( SELECT COUNT(TARGETAMOUNT_NORMALIZEDAMOUNT_AMOUNT) FROM OfferValidation where TARGETAMOUNT_NORMALIZEDAMOUNT_AMOUNT='C.105') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','TargetNormalizedAmountCode','C.106',@OPPCOUNT,( SELECT COUNT(TARGETAMOUNT_NORMALIZEDAMOUNT_CODE_NAME) FROM OfferValidation where TARGETAMOUNT_NORMALIZEDAMOUNT_CODE_NAME='C.106') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','TargetNormalizedAmountCode','C.108',@OPPCOUNT,( SELECT COUNT(`TARGETAMOUNT_NORMALIZEDAMOUNT_AMOUNT&TARGETAMOUNT_CODE_NAME`) FROM OfferValidation where `TARGETAMOUNT_NORMALIZEDAMOUNT_AMOUNT&TARGETAMOUNT_CODE_NAME`='C.108') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','BATCHTYPE Name','L.101',@OPPCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_BATCHTYPE_VALUE_NAME) FROM OfferValidation where EXTENSIONS_MASTER_BATCHTYPE_VALUE_NAME='L.101') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','BATCHTYPE DisplayName','L.102',@OPPCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_BATCHTYPE_VALUE_DISPLAYNAME) FROM OfferValidation where EXTENSIONS_MASTER_BATCHTYPE_VALUE_DISPLAYNAME='L.102') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','BATCHTYPE Key','L.103',@OPPCOUNT,( SELECT COUNT(EXTENSIONS_MASTER_BATCHTYPE_VALUE_KEY) FROM OfferValidation where EXTENSIONS_MASTER_BATCHTYPE_VALUE_KEY='L.103') ,'ERROR',NOW()
    UNION ALL
    select TENANT,'Offer','StartDate','D.101',@OPPCOUNT,( SELECT COUNT(STARTDATE) FROM OfferValidation where STARTDATE='D.101'),'WARN',NOW()
    UNION ALL
    select TENANT,'Offer','StartDate','D.102',@OPPCOUNT,( SELECT COUNT(STARTDATE) FROM OfferValidation where STARTDATE='D.102'),'WARN',NOW()
    UNION ALL
    select TENANT,'Offer','StartDate','D.103',@OPPCOUNT,( SELECT COUNT(STARTDATE) FROM OfferValidation where STARTDATE='D.103'),'WARN',NOW()
    UNION ALL
    select TENANT,'Offer','Enddate','D.101',@OPPCOUNT,( SELECT COUNT(ENDDATE) FROM OfferValidation where ENDDATE='D.101'),'WARN',NOW()
    UNION ALL
    select TENANT,'Offer','Enddate','D.102',@OPPCOUNT,( SELECT COUNT(ENDDATE) FROM OfferValidation where ENDDATE='D.102'),'WARN',NOW()
    UNION ALL
    select TENANT,'Offer','Enddate','D.103',@OPPCOUNT,( SELECT COUNT(ENDDATE) FROM OfferValidation where ENDDATE='D.103'),'WARN',NOW()
    UNION ALL
    select TENANT,'Offer','TargetDate','D.101',@OPPCOUNT,( SELECT COUNT(TARGETDATE) FROM OfferValidation where TARGETDATE='D.101'),'WARN',NOW()
    UNION ALL
    select TENANT,'Offer','TargetDate','D.102',@OPPCOUNT,( SELECT COUNT(TARGETDATE) FROM OfferValidation where TARGETDATE='D.102'),'WARN',NOW()
    UNION ALL
    select TENANT,'Offer','TargetDate','D.103',@OPPCOUNT,( SELECT COUNT(TARGETDATE) FROM OfferValidation where TARGETDATE='D.103'),'WARN',NOW()
    UNION ALL
    SELECT TENANT,'Offer','quote','M.102',@OPPCOUNT,( SELECT COUNT(`quote`) FROM OfferValidation where `quote`='M.102') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Offer','quote','M.101',@OPPCOUNT,( SELECT COUNT(quotem101) FROM OfferValidation where quotem101='M.101') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Offer','customer','M.102',@OPPCOUNT,( SELECT COUNT(customer) FROM OfferValidation where customer='M.102') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Offer','customer','M.101',@OPPCOUNT,( SELECT COUNT(customerm101) FROM OfferValidation where customerm101='M.101') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Offer','Product','M.102',@OPPCOUNT,( SELECT COUNT(Product) FROM OfferValidation where Product='M.102') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Offer','Product','M.101',@OPPCOUNT,( SELECT COUNT(Productm101) FROM OfferValidation where Productm101='M.101') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Offer','predecessor','M.102',@OPPCOUNT,( SELECT COUNT(predecessor) FROM OfferValidation where predecessor='M.102') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Offer','predecessor','M.101',@OPPCOUNT,( SELECT COUNT(predecessorm101) FROM OfferValidation where predecessorm101='M.101') ,'ERROR',NOW()
    UNION ALL
    SELECT TENANT,'Offer','Reseller','M.102',@OPPCOUNT,( SELECT COUNT(reseller) FROM OfferValidation where reseller='M.102') ,'WARN',NOW()
    UNION ALL
    SELECT TENANT,'Offer','Distributor','M.101',@OPPCOUNT,( SELECT COUNT(distributor) FROM OfferValidation where distributor='M.101') ,'WARN',NOW();
END;
