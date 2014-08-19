tenant=$1
url=$2
echo $(date +'%r')
cd ~/
cd /data_raid10/software/dell-export/GoodData-Customized
if [ ! -d "$tenant" ] ; then
mkdir $tenant $tenant/internal $tenant/CSV $tenant/Json $tenant/Logs $tenant/Prev_dates $tenant/zip_backup
cp Config/*.map $tenant/internal
fi

if [ $tenant == 'dell' ]; then

node GetJson.js  $tenant $url  'app.opportunities' '{"filter":{"type": "app.opportunity","isSubordinate":"false","extensions.master.country.value.name": {"$in": ["CZ","DK","FI","GR","HU","PT","RO","SI","IL","KZ","LT","RU","UA","NL","NO","PL","SK","SE","TR"]},"relationships.assignedTeam.targets.displayName": { "$regex": "^HPS"}},"params":{"stream":"true","limit":-1}}' 'app.opportunity.json' 'bill.moor' 'passwordone' 'find'
rm $tenant/Json/*.json

#node GetJson.js $tenant $url 'app.quotes' '{"params":{"stream":"true","limit":-1}}' 'app.quote.json' 'bill.moor' 'passwordone' 'find'
#rm $tenant/Json/*.json

#node GetJson.js $tenant $url 'app.offers' '{"params":{"stream":"true","limit":-1}}' 'app.offer.json' 'bill.moor' 'passwordone' 'find'
#rm $tenant/Json/*.json

node GetJson.js $tenant $url 'app.lookups' '{"filter":{"group": "TargetSelling"},"params":{"stream":"true","limit":-1}}' 'FiscalCalendar.json' 'bill.moor' 'passwordone' 'find'
rm $tenant/Json/*.json

cp dell/CSV/dummy/*.csv dell/CSV


elif [ $tenant != 'dell' ]; then

node GetJson.js $tenant $url 'app.offers' '{"params":{"stream":"true","limit":-1}}' 'app.offer.json' 'bill.moor' 'passwordone' 'find'
rm $tenant/Json/*.json

node GetJson.js  $tenant $url 'app.opportunities' '{"filter":{"isSubordinate":"false"},"params":{"stream":"true","limit":-1}}' 'app.opportunity.json' 'bill.moor' 'passwordone' 'find'
#node GetJson.js $tenant $url 'app.opportunities' '{"params":{"stream":"true","limit":-1}}' 'app.opportunity.json' 'bill.moor' 'passwordone' 'find'
rm $tenant/Json/*.json

node GetJson.js $tenant $url 'app.assets' '{"filter":{"type":"app.asset/service"},"params":{"stream":"true","limit":-1}}' 'app.asset.json' 'bill.moor' 'passwordone' 'find'
rm $tenant/Json/*.json

node GetJson.js $tenant $url 'app.bookings' '{"params":{"stream":"true","limit":-1}}' 'app.booking.json' 'bill.moor' 'passwordone' 'find'
rm $tenant/Json/*.json

node GetJson.js $tenant $url 'app.quotes' '{"params":{"stream":"true","limit":-1}}' 'app.quote.json' 'bill.moor' 'passwordone' 'find'
rm $tenant/Json/*.json

node GetJson.js $tenant $url 'app.lookups' '{"filter":{"group": "TargetSelling"},"params":{"stream":"true","limit":-1}}' 'FiscalCalendar.json' 'bill.moor' 'passwordone' 'find'
rm $tenant/Json/*.json

fi

if [ $tenant == 'nielsen' ]; then

node GetJson.js  $tenant $url 'core.contacts' '{"filter":{"type":"core.contact/organization"},"params":{"stream":"true","limit":-1}}' 'app.contact.json' 'bill.moor' 'passwordone' 'find'
rm $tenant/Json/*.json

fi


<<NoDeleteRecords
node GetJson.js $tenant $url 'app.offers' '{"params":{"stream":"true","limit":-1}}' 'deletedOffers.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenant/Json/*.json
node GetJson.js $tenant $url 'app.opportunities' '{"params":{"stream":"true","limit":-1}}' 'deletedOpportunities.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenant/Json/*.json
node GetJson.js $tenant $url 'app.assets' '{"filter":{"type":"app.asset/service"},"params":{"stream":"true","limit":-1}}' 'deletedAsset.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenant/Json/*.json
node GetJson.js $tenant $url 'app.bookings' '{"params":{"stream":"true","limit":-1}}' 'deletedBooking.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenant/Json/*.json
node GetJson.js $tenant $url 'app.quotes' '{"params":{"stream":"true","limit":-1}}' 'deletedQuote.json' 'bill.moor' 'passwordone' 'finddeleted'
rm $tenant/Json/*.json
node GetJson.js $tenant $url 'app.lookups' '{"filter":{"group": "TargetSelling"},"params":{"stream":"true","limit":-1}}' 'deletedFiscalCalendar.json' 'bill.moor' 'passwordone' 'finddeleted' 
rm $tenant/Json/*.json
NoDeleteRecords

now=$(date +'%Y%m%d.%H%M')
#now=$(date +"%d_%h_%Y")
cd $tenant/Json
rm *.json
cd ../CSV


<<commentTr
if [ $tenant == 'guidance' ]; then
grep -v Transitioned app.opportunity.csv > app.opportunity.tmp
cp app.opportunity.tmp app.opportunity.csv
grep -v Transitioned app.offer.csv > app.offer.tmp
cp app.opportunity.tmp app.opportunity.csv
grep -v Transitioned app.quote.csv > app.quote.tmp
cp app.quote.tmp app.quote.csv
fi
commentTr

rm ../zip_backup/*.zip
mv *.zip ../zip_backup/

zip GoodData.full.$tenant.$now.$url.zip *.csv
#zip GoodData.full.$tenant.$now.zip *.csv
rm *.csv


zipfile="GoodData.full.$tenant.$now.$url.zip"
#zipfile="GoodData.full.$tenant.$now.zip"
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




fi

if [ "$projectid" != '' ]; then
curl -i -u 'GD_Manager@servicesource.com:passwordone' --upload-file $zipfile https://na1-di.gooddata.com/project-uploads/$projectid/ --insecure --connect-timeout 10000 --max-time 10000
fi
echo $(date +"%Y-%m-%d") > dateLog_Deleted.config
echo $(date +'%r')

