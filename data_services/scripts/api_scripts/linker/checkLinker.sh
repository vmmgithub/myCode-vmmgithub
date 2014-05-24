#!/bin/bash

tenant="$1";
url="$2";


if [[ -z $tenant || -z $url ]]
then
echo "Usage: $0 <tenant> <url>"
exit 1
fi

#   Adding host name , same tenant can exists accross host (dev/stg/prod/uat)
count=`ps -eaf | grep cruiseControl | grep $tenant |grep $url | grep -v 'No records found' | wc -l`

if [[ $count -eq 0 ]]
then
echo "Linker is not running";
else
echo "Linker is running";
fi
