#!/bin/bash

if [[ "$1" == "" || "$2" == "" ]]; then
   echo "Usage: ./startLinker.sh <tenant> <limit> <match> [ <URL> ]"
   echo "Dell Usage: ./startLinker.sh <tenant> <limit> <match> [ <cluster1 URL> , <cluster2 URL> ]"
   exit 1
fi

tenant="$1"
url="$4"
logs="logs"
if [[ $url = '' ]]; then
. ./env.sh $1
fi

cmd="./scripts/linker.js --operation cruiseControl --tenant $tenant "

if [[ -n $2 ]]
then
        cmd="${cmd} --limit $2 "
fi

if [[ -n $3 ]]
then
        cmd="${cmd} --match $3 "
fi

mkdir -p ${logs}

if [[ $tenant == 'dell' ]]
then
        if [[ -n $4 ]]
        then
                c="${cmd} --host $4 "
                ${c} >> ${logs}/${tenant}.log &
                if [[ -n $5 ]]
                then
                        sleep 30
                        c="${cmd} --host $5 "
                        ${c} >> ${logs}/${tenant}.log &
                fi
        else
                c="${cmd} --host dell-prd1dl3-int.ssi-cloud.com "
                ${c} >> ${logs}/${tenant}.log &
                sleep 30

                c="${cmd} --host dell-prd1dl4-int.ssi-cloud.com "
                ${c} >> ${logs}/${tenant}.log &
                #sleep 30
        fi
else
        cmd="${cmd} --host $url "
        ${cmd} >> ${logs}/${tenant}.log &
fi
