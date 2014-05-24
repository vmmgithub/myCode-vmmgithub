#!/bin/bash
tenant=polycom
host="prod02dl-int.ssi-cloud.com"
#host="stgcurrent.ssi-cloud.com"
user="bill.moor@polycom.com"
port=443
passwd=passwordone
dt=`date +%Y%m%d`

scriptDir="/data_raid10/software/Implementations/data_services/scripts/api_scripts/tenants/polycom"
logDir="/data_raid10/software/Implementations/data_services/data/prd/logs"
logFile="polycomLog".$dt
nohup echo "start time =>"`date "+DATE: %Y-%m-%d TIME: %H:%M:%S"` > $logDir/$logFile; $scriptDir/polycomListPriceUpd.js -t $tenant -h $host -u $user -n $port -p $passwd >> $logDir/$logFile; echo "Stop time =>"`date "+DATE: %Y-%m-%d TIME: %H:%M:%S"` >> $logDir/$logFile &
