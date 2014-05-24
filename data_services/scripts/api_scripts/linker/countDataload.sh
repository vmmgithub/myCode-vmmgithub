#!/bin/bash

tenant="$1"
url="$4"
if [[ $url = '' ]]; then
. ./env.sh $1
fi

cmd="./scripts/linker.js --operation countJobs --host $url --tenant $tenant "

if [[ -n $2 ]]
then
cmd="${cmd} --match $2 "
fi

if [[ -n $3 ]]
then
cmd="${cmd} --incomplete false "
fi

${cmd}
