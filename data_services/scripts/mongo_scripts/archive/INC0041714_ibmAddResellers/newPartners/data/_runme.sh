#!/bin/bash -e

echo
echo '*** START AT' `date` '***'
echo params $*

echo
if ! which node >/dev/null 2>&1; then
    echo no node binary found
    exit 1
fi

if ! which mongo >/dev/null 2>&1; then
    echo no mongo binary found
    exit 1
fi

node ./checkParms.js $*

#node ./addReseller.js config=addReseller-swit.conf.json $*

node ./addReseller.js config=addReseller-aus.conf.json $*

#node ./lineItemsFix.js $*

echo
echo '*** FINISH AT' `date` '***'
echo

if which mail >/dev/null 2>&1; then
    echo mailing output to developers
    cat ./output.log | mail -s 'addReseller output' vklenov@silvertreesystems.com
fi

echo '*** PLEASE RESTART ALL NODE SERVERS TO MAKE RESELLER MEMBERS WORK ***'
echo

