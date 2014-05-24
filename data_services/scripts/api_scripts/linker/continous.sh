#!/bin/bash

tenant="$1";
url="$2";
logs="logs"
stop="${logs}/$1.$2.stop"
limit=1


if [[ -z $tenant || -z $url ]]
then
echo "Usage: $0 <tenant> <url>"
exit 1
fi

while true 
do

#   pending jobs are  selected for  processing 

# type=`./countDataload.sh ${tenant} 2>/dev/null | grep pending | cut -d'[' -f5 | cut -d']' -f1 | head -1`
# Replacing countDataload.sh by following command to pass hostname 
type=`./scripts/linker.js --operation countJobs --host ${url} --tenant ${tenant} 2>/dev/null | grep pending | cut -d'[' -f5 | cut -d']' -f1 | head -1`

#  checks linker status (running = 0 / not running = 1)
running=`./checkLinker.sh ${tenant} ${url} 2>/dev/null | grep not | wc -l`

if [[ -n "$type" && $running -ne 0 ]]
then

if [[ -f "$stop" ]]
then
echo "See the stop file and getting out for ${tenant} ... "
rm "$stop"
exit 0;
fi

date=`date "+%DT%T"`
echo "[$date] Restarting linking for $tenant" 
`./startLinker.sh "${tenant}" "${limit}" "${type}" "${url}"`
else
date=`date "+%DT%T"`
echo "[$date] Linking already running for $tenant" 
fi

sleep 180
done
