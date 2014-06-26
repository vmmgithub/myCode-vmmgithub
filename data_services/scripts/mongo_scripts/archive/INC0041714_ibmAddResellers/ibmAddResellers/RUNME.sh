#!/bin/bash

cd `dirname $0`
cd data || exit 1

echo '*** ALL SCRIPT OUTPUT ALSO GOES TO ./output.log ***'

./_runme.sh $* 2>&1 | tee -a ./output.log