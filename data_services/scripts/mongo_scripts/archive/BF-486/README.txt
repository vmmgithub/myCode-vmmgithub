tenant: ibm
scrub finds all opportunities without resultReason extension and set resultReason to 'none'

usage:
./index.js host=127.0.0.1 port=7000 ssl=0 doit=1

where

host - api host
port - api port
ssl [0/1 ] - use ssl connection to api
doit [0/1 ] - turn off dry run mode and make actual update