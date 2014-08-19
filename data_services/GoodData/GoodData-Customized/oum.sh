cd ~/
cd /data_raid10/software/dell-export/GoodData-Customized/OUM
rm *.csv
rm *.zip

declare -a arr=(ibm siemens juniper bluecoat aspect bazaarvoice guidance btinet avispl nielsen dell perkinelmer vocus polycom cisco blackboard projectcristal  )

for Tenant in ${arr[@]}
do
echo $Tenant


if [ "$Tenant"  == 'ibm' ] || [ "$Tenant" == 'siemens' ] || [ "$Tenant" == 'dynapro' ] || [ "$Tenant" == 'juniper' ] || [ "$Tenant" == 'bluecoat' ]; then

cp /data_raid10/software/dell-export/GoodData/$Tenant/CSV/GoodData.full.*.zip /data_raid10/software/dell-export/GoodData-Customized/OUM

else

cp /data_raid10/software/dell-export/GoodData-Customized/$Tenant/CSV/GoodData.full.*.zip /data_raid10/software/dell-export/GoodData-Customized/OUM

fi


unzip *$Tenant*.zip
#mv app.asset.csv $Tenant.asset.csv
#mv app.booking.csv $Tenant.booking.csv
#mv app.offer.csv $Tenant.offer.csv
mv app.opportunity.csv $Tenant.opportunity.csv
#mv app.quote.csv $Tenant.quote.csv

rm deleted*
rm app.contact.csv
rm app.activity.csv
rm app.asset.csv 
rm app.booking.csv 
rm app.offer.csv
rm app.quote.csv
rm FiscalCalendar.csv


rm *$Tenant*.zip 

if [ "$Tenant" != 'ibm' ]; then
 
sed -i '1d' $Tenant.*.csv
#rm FiscalCalendar.csv
#else
#mv  FiscalCalendar.csv app.FiscalCalendar.csv

fi


#cat $Tenant.asset.csv >> asset.csv
#cat $Tenant.booking.csv >> booking.csv
#cat $Tenant.offer.csv >> offer.csv
cat $Tenant.opportunity.csv >> opportunity.csv
#cat $Tenant.quote.csv >> quote.csv
rm *$Tenant*

done

#mv asset.csv app.asset.csv 
#mv booking.csv app.booking.csv
#mv offer.csv app.offer.csv
mv opportunity.csv app.opportunity.csv
#mv quote.csv app.quote.csv

now=$(date +"%d_%h_%Y")
#cut -d"\",\"" -f1-7,10,11,13,14,16,19-24,30-33,37,38 app.opportunity.csv >> oum.csv
awk -v FS="\",\"" '{ print  $1 "\",\""  $2 "\",\"" $3 "\",\"" $4 "\",\"" $5 "\",\"" $6 "\",\"" $7 "\",\"" $10 "\",\"" $11 "\",\"" $13 "\",\"" $14 "\",\"" $16 "\",\""  $19 "\",\"" $20 "\",\""  $21 "\",\"" $22 "\",\"" $23 "\",\"" $24 "\",\"" $30 "\",\"" $31 "\",\"" $32 "\",\"" $33 "\",\"" $37 "\",\"" $38  "\"" }' < app.opportunity.csv >> oum.csv
zip OUM.$now.zip oum.csv
rm *.csv

<<SFTP
curl -i -u 'gooddata@servicesource.com:passwordone' --upload-file GoodData.full.all.$now.zip  https://secure-di.gooddata.com/project-uploads/kwcdzr5z8gpyrp9w92yov3401wwlscay/ --insecure --connect-timeout 10000 --max-time 10000
SFTP

