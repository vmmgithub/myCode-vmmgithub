tenent=$1
url=$2
echo $(date %r);
now=$(date +"%d_%h_%Y")
cd ~/
cd /data_raid10/software/dell-export/GoodData-Customized

IN=$(cat ./$tenent/internal/dateLog_app.offer.config)
arrIN=(${IN//,/ })
node GetJson.js $tenent $url 'app.offers' '{"filter":{"$or":[{"systemProperties.lastModifiedOn":{"$gt":"'${arrIN[1]}'"}},{"systemProperties.createdOn":{"$gt":"'${arrIN[0]}'"}}]},"params":{"stream":"true","limit":-1}}' 'app.offer.json' 'bill.moor' 'passwordone' 'find'


IN=$(cat ./$tenent/internal/dateLog_app.opportunity.config)
arrIN=(${IN//,/ })
node GetJson.js $tenent $url 'app.opportunities' '{"filter":{"$or":[{"systemProperties.lastModifiedOn":{"$gt":"'${arrIN[1]}'"}},{"systemProperties.createdOn":{"$gt":"'${arrIN[0]}'"}}]},"params":{"stream":"true","limit":-1}}' 'app.opportunity.json' 'bill.moor' 'passwordone' 'find'

IN=$(cat ./$tenent/internal/dateLog_app.asset.config)
arrIN=(${IN//,/ })
node GetJson.js $tenent $url 'app.assets' '{"filter":{"type":"app.asset/service","$or":[{"systemProperties.lastModifiedOn":{"$gt":"'${arrIN[1]}'"}},{"systemProperties.createdOn":{"$gt":"'${arrIN[0]}'"}}]},"params":{"stream":"true","limit":-1}}' 'app.asset.json' 'bill.moor' 'passwordone' 'find'

IN=$(cat ./$tenent/internal/dateLog_app.booking.config)
arrIN=(${IN//,/ })
node GetJson.js $tenent $url 'app.bookings' '{"filter":{"$or":[{"systemProperties.lastModifiedOn":{"$gt":"'${arrIN[1]}'"}},{"systemProperties.createdOn":{"$gt":"'${arrIN[0]}'"}}]},"params":{"stream":"true","limit":-1}}' 'app.booking.json' 'bill.moor' 'passwordone' 'find'
IN=$(cat ./$tenent/internal/dateLog_app.quote.config)
arrIN=(${IN//,/ })
node GetJson.js $tenent $url 'app.quotes' '{"filter":{"$or":[{"systemProperties.lastModifiedOn":{"$gt":"'${arrIN[1]}'"}},{"systemProperties.createdOn":{"$gt":"'${arrIN[0]}'"}}]},"params":{"stream":"true","limit":-1}}' 'app.quote.json' 'bill.moor' 'passwordone' 'find'
IN=$(cat ./$tenent/internal/dateLog_FiscalCalendar.config)
arrIN=(${IN//,/ })
node GetJson.js $tenent $url 'app.lookups' '{"filter":{"group": "TargetSelling","$or":[{"systemProperties.lastModifiedOn":{"$gt":"'${arrIN[1]}'"}},{"systemProperties.createdOn":{"$gt":"'${arrIN[0]}'"}}]},"params":{"stream":"true","limit":-1}}' 'FiscalCalendar.json' 'bill.moor' 'passwordone' 'find'

if [ $tenent == 'nielsen' ]; then
IN=$(cat ./$tenent/internal/dateLog_app.contact.config)
arrIN=(${IN//,/ })
node GetJson.js  $tenent $url 'core.contacts' '{"filter":{"type":"core.contact/organization","$or":[{"systemProperties.lastModifiedOn":{"$gt":"'${arrIN[1]}'"}},{"systemProperties.createdOn":{"$gt":"'${arrIN[0]}'"}}]},"params":{"stream":"true","limit":-1}}' 'app.contact.json' 'bill.moor' 'passwordone' 'find'
fi

<< OldCode node GetJson.js $tenent $url 'app.offers' '{"params":{"stream":"true","limit":-1}}' 'deletedOffers.json' 'bill.moor' 'passwordone' 'finddeleted'
node GetJson.js $tenent $url 'app.opportunities' '{"params":{"stream":"true","limit":-1}}' 'deletedOpportunities.json' 'bill.moor' 'passwordone' 'finddeleted'
node GetJson.js $tenent $url 'app.assets' '{"filter":{"type":"app.asset/service"},"params":{"stream":"true","limit":-1}}' 'deletedAsset.json' 'bill.moor' 'passwordone' 'finddeleted'
node GetJson.js $tenent $url 'app.bookings' '{"params":{"stream":"true","limit":-1}}' 'deletedBooking.json' 'bill.moor' 'passwordone' 'finddeleted'
node GetJson.js $tenent $url 'app.quotes' '{"params":{"stream":"true","limit":-1}}' 'deletedQuote.json' 'bill.moor' 'passwordone' 'finddeleted'
node GetJson.js $tenent $url 'app.lookups' '{"filter":{"group": "TargetSelling"},"params":{"stream":"true","limit":-1}}' 'deletedFiscalCalendar.json' 'bill.moor' 'passwordone' 'finddeleted'
OldCode
IN=$(cat ./$tenent/internal/dateLog_Deleted.config)
echo $IN
node GetJson.js $tenent $url 'app.offers' '{"filter":{"systemProperties.expiredOn":{"$gt":"'$IN'"}},"params":{"stream":"true","limit":-1}}' 'deletedOffers.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenent/Json/*.json

node GetJson.js $tenent $url 'app.opportunities' '{"filter":{"systemProperties.expiredOn":{"$gt":"'$IN'"}},"params":{"stream":"true","limit":-1}}' 'deletedOpportunities.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenent/Json/*.json

node GetJson.js $tenent $url 'app.assets' '{"filter":{"type":"app.asset/service","systemProperties.expiredOn":{"$gt":"'$IN'"}},"params":{"stream":"true","limit":-1}}' 'deletedAsset.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenent/Json/*.json

node GetJson.js $tenent $url 'app.bookings' '{"filter":{"systemProperties.expiredOn":{"$gt":"'$IN'"}},"params":{"stream":"true","limit":-1}}' 'deletedBooking.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenent/Json/*.json


node GetJson.js $tenent $url 'app.quotes' '{"filter":{"systemProperties.expiredOn":{"$gt":"'$IN'"}},"params":{"stream":"true","limit":-1}}' 'deletedQuote.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenent/Json/*.json

node GetJson.js $tenent $url 'app.lookups' '{"filter":{"group": "TargetSelling","systemProperties.expiredOn":{"$gt":"'$IN'"}},"params":{"stream":"true","limit":-1}}' 'deletedFiscalCalendar.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenent/Json/*.json

if [ $tenent == 'nielsen' ]; then
node GetJson.js $tenent $url 'core.contacts' '{"filter":{"type":"core.contact/organization","systemProperties.expiredOn":{"$gt":"'$IN'"}},"params":{"stream":"true","limit":-1}}' 'deletedAsset.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenent/Json/*.json
fi


cd $tenent/Json
cd ../CSV/
mv *.zip ../zip_backup/
zip GoodData.Incr.$tenent.$now.zip *.csv

rm *.csv

zipfile="GoodData.Incr.$tenent.$now.zip"
projectid=''
if [ "$tenent" == 'aspect' ]; then
projectid='gffmhadbkt9uw0uqoclwiv4te4coxfcg'
elif [ "$tenent" == 'bazaarvoice' ]; then
projectid='k3jsucvjz32gi2a4145w9i0afafue7li'
elif [ "$tenent" == 'guidance' ]; then
projectid='dcsgzctad6fj25gyp3m6w7fg7eknh4vx'
elif [ "$tenent" == 'siemens' ]; then
projectid='hbubyhehxpkwfbeo843xvluwwkguidxu'
elif [ "$tenent" == 'juniper' ]; then
projectid='v8ktraexp9s4cjbdlpxa16q88p0fw81b'
elif [ "$tenent" == 'ibm' ]; then
projectid='zpdhxtkca5sen14udgdj76cfh35naixo'
elif [ "$tenent" == 'dynapro' ]; then
projectid='a04rfojbzvitius3rk0u9jgdo248sr3w'
elif [ "$tenent" == 'bluecoat' ]; then
projectid='lqi15kcmqde94f1fzmpay1uqjdotci6r'
elif [ "$tenent" == 'btinet' ]; then
projectid='p1vhno77w25l2bk70skt0d6keic3zxn1'
elif [ "$tenent" == 'avispl' ]; then
projectid='k1n4i61d48v6eoybgdpzb93fkvgt0io5'
elif [ "$tenent" == 'projectcristal' ]; then
projectid='tlh9gep2vlo1r8dk9yvitar857kte7gd'
elif [ "$tenent" == 'dell' ]; then
projectid='zfs2b4u556acg4vnyiiuckqbaan099jq'
elif [ "$tenent" == 'nielsen' ]; then
projectid='f62wm4vpqhmbaj8fe14jo6q8u1nhav32'
fi

if [ "$projectid" != '' ]; then
curl -i -u 'gooddata@servicesource.com:passwordone' --upload-file $zipfile https://secure-di.gooddata.com/project-uploads/$projectid/ --insecure --connect-timeout 10000 --max-time 10000
fi

echo $(date +"%Y-%m-%d") > dateLog_Deleted.config
echo $(date %r)
