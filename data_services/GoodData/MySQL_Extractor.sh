#!/bin/bash

tenant=$1

now=$(date +'%Y%m%d.%H%M')

cd /tmp
if [ ! -d "outputGoodData" ] ; then
mkdir outputGoodData
chmod 777 outputGoodData
fi



mysql <<EOFMYSQL

SELECT 'AssetPK', 'ExistingStartDate', 'ExistingEndDate', 'ExistingServiceProduct', 'ExistingCoveredProduct', 'Tenant', 'CUSTOMFIELD1', 'CUSTOMFIELD2', 'CUSTOMFIELD3', 'CUSTOMFIELD4', 'CUSTOMFIELD5', 'CUSTOMFIELD6', 'CUSTOMFIELD7', 'CUSTOMFIELD8', 'CUSTOMFIELD9', 'CUSTOMFIELD10', 'CUSTOMFIELD11', 'CUSTOMFIELD12', 'CUSTOMFIELD13', 'CUSTOMFIELD14', 'CUSTOMFIELD15', 'CUSTOMFIELD16', 'CUSTOMFIELD17', 'CUSTOMFIELD18', 'CUSTOMFIELD19', 'CUSTOMFIELD20', 'CUSTOMFACT1', 'CUSTOMFACT2', 'CUSTOMFACT3', 'CUSTOMFACT4', 'CUSTOMFACT5'
UNION ALL
SELECT AssetPK, ExistingStartDate, ExistingEndDate, ExistingServiceProduct, ExistingCoveredProduct, Tenant, CUSTOMFIELD1, CUSTOMFIELD2, CUSTOMFIELD3, CUSTOMFIELD4, CUSTOMFIELD5, CUSTOMFIELD6, CUSTOMFIELD7, CUSTOMFIELD8, CUSTOMFIELD9, CUSTOMFIELD10, CUSTOMFIELD11, CUSTOMFIELD12, CUSTOMFIELD13, CUSTOMFIELD14, CUSTOMFIELD15, CUSTOMFIELD16, CUSTOMFIELD17, CUSTOMFIELD18, CUSTOMFIELD19, CUSTOMFIELD20, CUSTOMFACT1, CUSTOMFACT2, CUSTOMFACT3, CUSTOMFACT4, CUSTOMFACT5 
FROM $tenant.GDV_ASSETS 
INTO OUTFILE '/tmp/outputGoodData/tmp_$tenant.assets.$now.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT 'BookingPK', 'Tenant', 'ClientBookingDate', 'AssetPK', 'CUSTOMFIELD1', 'CUSTOMFIELD2', 'CUSTOMFIELD3', 'CUSTOMFIELD4', 'CUSTOMFIELD5', 'CUSTOMFIELD6', 'CUSTOMFIELD7', 'CUSTOMFIELD8', 'CUSTOMFIELD9', 'CUSTOMFIELD10', 'CUSTOMFIELD11', 'CUSTOMFIELD12', 'CUSTOMFIELD13', 'CUSTOMFIELD14', 'CUSTOMFIELD15', 'CUSTOMFIELD16', 'CUSTOMFIELD17', 'CUSTOMFIELD18', 'CUSTOMFIELD19', 'CUSTOMFIELD20', 'CUSTOMFACT1', 'CUSTOMFACT2', 'CUSTOMFACT3', 'CUSTOMFACT4', 'CUSTOMFACT5'
UNION ALL
SELECT BookingPK, Tenant, ClientBookingDate, AssetPK, CUSTOMFIELD1, CUSTOMFIELD2, CUSTOMFIELD3, CUSTOMFIELD4, CUSTOMFIELD5, CUSTOMFIELD6, CUSTOMFIELD7, CUSTOMFIELD8, CUSTOMFIELD9, CUSTOMFIELD10, CUSTOMFIELD11, CUSTOMFIELD12, CUSTOMFIELD13, CUSTOMFIELD14, CUSTOMFIELD15, CUSTOMFIELD16, CUSTOMFIELD17, CUSTOMFIELD18, CUSTOMFIELD19, CUSTOMFIELD20, CUSTOMFACT1, CUSTOMFACT2, CUSTOMFACT3, CUSTOMFACT4, CUSTOMFACT5 
FROM $tenant.GDV_BOOKINGS
INTO OUTFILE '/tmp/outputGoodData/tmp_$tenant.bookings.$now.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT 'OfferPK',' OfferRenewalAmountUSD','OfferTransactionAmountUSD','OfferRenewalAmountLocal','OfferTransactionAmountLocal','Tenant','BatchType','CreatedBy','ModifiedBy','ResultReason','AssetPK','QuotePK','NewServiceProduct','LocalRenewalCurrency','LocalTransactionCurrency','CreatedOnDate','ModifiedDate','NewStartDate','NewEndDate','SalesStage','CUSTOMFIELD1','CUSTOMFIELD2','CUSTOMFIELD3','CUSTOMFIELD4','CUSTOMFIELD5','CUSTOMFIELD6','CUSTOMFIELD7','CUSTOMFIELD8','CUSTOMFIELD9','CUSTOMFIELD10','CUSTOMFIELD11','CUSTOMFIELD12','CUSTOMFIELD13','CUSTOMFIELD14','CUSTOMFIELD15','CUSTOMFIELD16','CUSTOMFIELD17','CUSTOMFIELD18','CUSTOMFIELD19','CUSTOMFIELD20','CUSTOMFACT1','CUSTOMFACT2','CUSTOMFACT3','CUSTOMFACT4','CUSTOMFACT5','CUSTOMDATE1'
union
SELECT OfferPK, OfferRenewalAmountUSD, OfferTransactionAmountUSD, OfferRenewalAmountLocal, OfferTransactionAmountLocal, Tenant, BatchType, CreatedBy, ModifiedBy, ResultReason, AssetPK, QuotePK, NewServiceProduct, LocalRenewalCurrency, LocalTransactionCurrency, CreatedOnDate, ModifiedDate, NewStartDate, NewEndDate, SalesStage, CUSTOMFIELD1, CUSTOMFIELD2, CUSTOMFIELD3, CUSTOMFIELD4, CUSTOMFIELD5, CUSTOMFIELD6, CUSTOMFIELD7, CUSTOMFIELD8, CUSTOMFIELD9, CUSTOMFIELD10, CUSTOMFIELD11, CUSTOMFIELD12, CUSTOMFIELD13, CUSTOMFIELD14, CUSTOMFIELD15, CUSTOMFIELD16, CUSTOMFIELD17, CUSTOMFIELD18, CUSTOMFIELD19, CUSTOMFIELD20, CUSTOMFACT1, CUSTOMFACT2, CUSTOMFACT3, CUSTOMFACT4, CUSTOMFACT5, CUSTOMDATE1 
FROM $tenant.GDV_OFFERS
INTO OUTFILE '/tmp/outputGoodData/tmp_$tenant.offers.$now.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT 'OpportunityPK','OpportunityRenewalAmountUSD','OpportunityTransactionAmountUSD','OpportunityRenewalAmountLocal','OpportunityTransactionAmountLocal','Tenant','CommitLevel','CreatedBy','ModifiedBy','SalesStage','OpportunityName','DirectChannel','LocalRenewalCurrency','LocalTransactionCurrency','CreatedOnDate','EarliestExistingEndDate','EstimatedCloseDate','ModifiedDate','ResolutionDate','ClientTheatre','BusinessLine','ClientRegion','ClientTerritory','Country','ExistingReseller','Customer','SalesRep','FirstContactDate','FirstQuoteDate','BookingDate','OpportunityType','BookingPK','QuotePK','ExistingDistributor','NewDistributor','NewReseller','EarliestNewStartDate','LatestNewEndDate','TransactionDate','SalesStageOrder','SellingPeriod','CUSTOMFIELD1','CUSTOMFIELD2','CUSTOMFIELD3','CUSTOMFIELD4','CUSTOMFIELD5','CUSTOMFIELD6','CUSTOMFIELD7','CUSTOMFIELD8','CUSTOMFIELD9','CUSTOMFIELD10','CUSTOMFIELD11','CUSTOMFIELD12','CUSTOMFIELD13','CUSTOMFIELD14','CUSTOMFIELD15','CUSTOMFIELD16','CUSTOMFIELD17','CUSTOMFIELD18','CUSTOMFIELD19','CUSTOMFIELD20','CUSTOMFACT1','CUSTOMFACT2','CUSTOMFACT3','CUSTOMFACT4','CUSTOMFACT5','CUSTOMDATE1' 
UNION
SELECT OpportunityPK, OpportunityRenewalAmountUSD, OpportunityTransactionAmountUSD, OpportunityRenewalAmountLocal, OpportunityTransactionAmountLocal, Tenant, CommitLevel, CreatedBy, ModifiedBy, SalesStage, OpportunityName, DirectChannel, LocalRenewalCurrency, LocalTransactionCurrency, CreatedOnDate, EarliestExistingEndDate, EstimatedCloseDate, ModifiedDate, ResolutionDate, ClientTheatre, BusinessLine, ClientRegion, ClientTerritory, Country, ExistingReseller, Customer, SalesRep, FirstContactDate, FirstQuoteDate, BookingDate, OpportunityType, BookingPK, QuotePK, ExistingDistributor, NewDistributor, NewReseller, EarliestNewStartDate, LatestNewEndDate, TransactionDate, SalesStageOrder, SellingPeriod, CUSTOMFIELD1, CUSTOMFIELD2, CUSTOMFIELD3, CUSTOMFIELD4, CUSTOMFIELD5, CUSTOMFIELD6, CUSTOMFIELD7, CUSTOMFIELD8, CUSTOMFIELD9, CUSTOMFIELD10, CUSTOMFIELD11, CUSTOMFIELD12, CUSTOMFIELD13, CUSTOMFIELD14, CUSTOMFIELD15, CUSTOMFIELD16, CUSTOMFIELD17, CUSTOMFIELD18, CUSTOMFIELD19, CUSTOMFIELD20, CUSTOMFACT1, CUSTOMFACT2, CUSTOMFACT3, CUSTOMFACT4, CUSTOMFACT5, CUSTOMDATE1 
FROM $tenant.GDV_OPPORTUNITIES
INTO OUTFILE '/tmp/outputGoodData/tmp_$tenant.opportunities.$now.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT 'QuotePK',' Tenant',' QuoteName',' CUSTOMFIELD1',' CUSTOMFIELD2',' CUSTOMFIELD3',' CUSTOMFIELD4',' CUSTOMFIELD5',' CUSTOMFIELD6',' CUSTOMFIELD7',' CUSTOMFIELD8',' CUSTOMFIELD9',' CUSTOMFIELD10',' CUSTOMFIELD11',' CUSTOMFIELD12',' CUSTOMFIELD13',' CUSTOMFIELD14',' CUSTOMFIELD15',' CUSTOMFIELD16',' CUSTOMFIELD17',' CUSTOMFIELD18',' CUSTOMFIELD19',' CUSTOMFIELD20',' CUSTOMFACT1',' CUSTOMFACT2',' CUSTOMFACT3',' CUSTOMFACT4',' CUSTOMFACT5'
UNION
SELECT QuotePK, Tenant, QuoteName, CUSTOMFIELD1, CUSTOMFIELD2, CUSTOMFIELD3, CUSTOMFIELD4, CUSTOMFIELD5, CUSTOMFIELD6, CUSTOMFIELD7, CUSTOMFIELD8, CUSTOMFIELD9, CUSTOMFIELD10, CUSTOMFIELD11, CUSTOMFIELD12, CUSTOMFIELD13, CUSTOMFIELD14, CUSTOMFIELD15, CUSTOMFIELD16, CUSTOMFIELD17, CUSTOMFIELD18, CUSTOMFIELD19, CUSTOMFIELD20, CUSTOMFACT1, CUSTOMFACT2, CUSTOMFACT3, CUSTOMFACT4, CUSTOMFACT5 
FROM $tenant.GDV_QUOTES
INTO OUTFILE '/tmp/outputGoodData/tmp_$tenant.quotes.$now.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

SELECT 'ClientCalendarPK', 'Tenant', 'Quarter', 'QuarterDescription', 'QuarterStartDate', 'QuarterEndDate'
UNION
SELECT ClientCalendarPK, Tenant, Quarter, QuarterDescription, QuarterStartDate, QuarterEndDate 
FROM gd_dynapro.FiscalCalendar
WHERE Tenant = '$tenant'
INTO OUTFILE '/tmp/outputGoodData/tmp_$tenant.fiscalcalendar.$now.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';

EOFMYSQL


cd /tmp/outputGoodData

rm mysql.GoodData.full.$tenant.*.zip



sed -e 's/0000-00-00 00:00:00//g;s:\\N::g;s/undefined//g' tmp_$tenant.assets.$now.csv > t.$tenant.assets.$now.csv
sed -e 's/0000-00-00 00:00:00//g;s:\\N::g;s/undefined//g' tmp_$tenant.bookings.$now.csv >t.$tenant.bookings.$now.csv
sed -e 's/0000-00-00 00:00:00//g;s:\\N::g;s/undefined//g' tmp_$tenant.offers.$now.csv > t.$tenant.offers.$now.csv
sed -e 's/0000-00-00 00:00:00//g;s:\\N::g;s/undefined//g' tmp_$tenant.opportunities.$now.csv > t.$tenant.opportunities.$now.csv
sed -e 's/0000-00-00 00:00:00//g;s:\\N::g;s/undefined//g' tmp_$tenant.quotes.$now.csv > t.$tenant.quotes.$now.csv


sed -e '/_ID/d' t.$tenant.assets.$now.csv > $tenant.assets.$now.csv
sed -e '/_ID/d' t.$tenant.bookings.$now.csv > $tenant.bookings.$now.csv
sed -e '/_ID/d' t.$tenant.offers.$now.csv > $tenant.offers.$now.csv
sed -e '/_ID/d' t.$tenant.opportunities.$now.csv > $tenant.opportunities.$now.csv
sed -e '/_ID/d' t.$tenant.quotes.$now.csv > $tenant.quotes.$now.csv



cat tmp_$tenant.fiscalcalendar.$now.csv > $tenant.fiscalcalendar.$now.csv

rm t*$tenant.*.csv

zip mysql.GoodData.full.$tenant.$now.zip *.csv

rm $tenant.*.csv


zipfile="mysql.GoodData.full.$tenant.$now.zip"
projectid=''

if [ "$tenant" == 'aspect' ]; then
projectid='gffmhadbkt9uw0uqoclwiv4te4coxfcg'
elif [ "$tenant" == 'bazaarvoice' ]; then
projectid='k3jsucvjz32gi2a4145w9i0afafue7li'
elif [ "$tenant" == 'guidance' ]; then
projectid='dcsgzctad6fj25gyp3m6w7fg7eknh4vx'
elif [ "$tenant" == 'siemens' ]; then
projectid='hbubyhehxpkwfbeo843xvluwwkguidxu'
elif [ "$tenant" == 'juniper' ]; then
projectid='v8ktraexp9s4cjbdlpxa16q88p0fw81b'
elif [ "$tenant" == 'ibm' ]; then
projectid='zpdhxtkca5sen14udgdj76cfh35naixo'
elif [ "$tenant" == 'dynapro' ]; then
projectid='a04rfojbzvitius3rk0u9jgdo248sr3w'
elif [ "$tenant" == 'bluecoat' ]; then
projectid='lqi15kcmqde94f1fzmpay1uqjdotci6r'
elif [ "$tenant" == 'btinet' ]; then
projectid='p1vhno77w25l2bk70skt0d6keic3zxn1'
elif [ "$tenant" == 'avispl' ]; then
projectid='k1n4i61d48v6eoybgdpzb93fkvgt0io5'
elif [ "$tenant" == 'projectcristal' ]; then
projectid='tlh9gep2vlo1r8dk9yvitar857kte7gd'
elif [ "$tenant" == 'dell' ]; then
projectid='zfs2b4u556acg4vnyiiuckqbaan099jq'
elif [ "$tenant" == 'nielsen' ]; then
projectid='f62wm4vpqhmbaj8fe14jo6q8u1nhav32'
elif [ "$tenant" == 'perkinelmer' ]; then
projectid='ee61760psdtru11ahkok1itp60vjaf1j'
elif [ "$tenant" == 'vocus' ]; then
projectid='y4jwblwqfpc9f77bdc3my4rb2c35aey7'
elif [ "$tenant" == 'polycom' ]; then
projectid='tkuc767dnzqbiwffhmfmx6vozvh0zfo2'
elif [ "$tenant" == 'cisco' ]; then
projectid='phiiqnbadd5ogsavndjowpznmizkufw3'
elif [ "$tenant" == 'blackboard' ]; then
projectid='jotb1zd4efv571ak0ufx98sy3e9caes0'
elif [ "$tenant" == 'googleinc' ]; then
projectid='qci42a7rmmxh2zgihzjf3nssbvjn75uu'
elif [ "$tenant" == 'aria' ]; then
projectid='ohe76bqtoxu718x26drxlggsc193c04p'



fi

if [ "$projectid" != '' ]; then
curl -i -u 'GD_Manager@servicesource.com:passwordone' --upload-file $zipfile https://na1-di.gooddata.com/project-uploads/$projectid/ --insecure --connect-timeout 10000 --max-time 10000
fi
echo $(date +"%Y-%m-%d") > dateLog_Deleted.config
echo $(date +'%r')
