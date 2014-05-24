#!/bin/bash

COLL="$1"
DLCONF="$2"
TYPE="$3"
LIMIT="$4"
IN="$3.$1"
OUT="$IN.relink.js"
LOG="$IN.relink.log"
DLIDTXT="$3.$1.dlids.txt"
DLIDS=""

if [[ -z $LIMIT ]]
then
LIMIT="100000"
fi

if [[ ! -f $DLIDTXT ]]
then
touch "$DLIDTXT"
fi

if [[ -f $OUT ]]
then
rm $OUT
fi

while read line
do
if [[ ! -z $line ]]
then
if [[ ! -z $DLIDS ]]
then
DLIDS="$DLIDS, '$line'"
else
DLIDS="'$line'"
fi
fi
done < $DLIDTXT
echo "Excluding these jobs $DLIDS ..."


echo "
var getUID = function(doc, type) {
  type = type || 'UID';
  var id;

  if (doc && doc.externalIds) {
    doc.externalIds.forEach(function(xid) {
       if (xid.schemeId.name == type) id = xid.id;
    });
  }
  return id;
}

var setUID = function(doc, type, id) {
  type = type || 'UID';

  if (doc && doc.externalIds) {
    doc.externalIds.forEach(function(xid) {
       if (xid.schemeId.name == type) xid.id = id;
    });
  }
}

var getRelUID = function(obj, relName) {
  if (obj && obj.relationships && obj.relationships[relName] && obj.relationships[relName].targets && 
    obj.relationships[relName].targets[0] && obj.relationships[relName].targets[0].id) {
        return obj.relationships[relName].targets[0].id;
  }
};

var newDlId = ObjectId();
var dlStart = ISODate();
var dlName = '$TYPE - Retry Linking';
var badQCount = 0;
var filter = { 
'systemProperties.tenant': 'dell', 
'systemProperties.expiredOn': ISODate('9999-01-01T00:00:00Z'),
 'type':'$TYPE',
 'systemProperties.qRank': 1,
'externalIds.id': {\$nin: [$DLIDS]}
};

var cursor = db.$COLL.find(filter).limit($LIMIT);

var i = 0;
while (cursor.hasNext()) {
  var doc = cursor.next();
  if (i++ % 1000 == 0) print('[' + ISODate() + '] Processed ' + i +' records with ' + badQCount + ' dlId ' + newDlId);

  var unresolvedLinks = false;
  for (relName in doc.relationships) {
    if (getRelUID(doc, relName)) {
      unresolvedLinks = true;
    }
  }

  if (unresolvedLinks && getUID(doc, 'batchLoad') != newDlId.valueOf()) {
    setUID(doc, 'batchLoad', newDlId.valueOf());
    doc.systemProperties.dlOn = ISODate();
    doc.systemProperties.qRank = 0;
    db.$COLL.save(doc);
    badQCount++;        
  }

}

if (badQCount !=0) {
  var dlConfig = db.app.dataload.configs.findOne({displayName: /$DLCONF/});
  dlConfig = {
    key: dlConfig._id.valueOf(),
    displayName: dlConfig.displayName,
    type: dlConfig.type
  }

  var endTime = ISODate();
  var dataload = {
      '_id' : newDlId,
      'displayName' : dlName,
      'inputSummary' : {
        'collectionSummary' : [
          {
            'collectionName' : '$COLL',
            'numberRecords' : badQCount
          }
        ],
        'dataSourceName' : 'Manual Retry'
      },
      'relationships' : {
        'dataloadConfig' : {
          'firstTarget' : dlConfig.displayName,
          'relation' : {
            'type' : 'core.lookup',
            'displayName' : 'Data Load Config',
            'name' : 'dataloadConfig',
            'key' : '50ac01a21f9c0c0000000748'
          },
          'targets' : [
            dlConfig
          ]
        }
      },
      'startTime' : dlStart,
      'endTime' : endTime,
      'loadEndTime' : endTime,
      'status' : {
        'key' : '50ac01a41f9c0c0000000a9d',
        'displayName' : 'Pending',
        'type' : 'app.lookup',
        'name' : 'pending'
      },
      'systemProperties' : {
        'createdBy' : 'bill.moor@dell.com',
        'createdOn' : dlStart,
        'expiredOn' : ISODate('9999-01-01T00:00:00Z'),
        'lastModifiedBy' : 'bill.moor@dell.com',
        'lastModifiedOn' : endTime,
        'qRank' : 4,
        'revisionId' : 1,
        'tenant' : 'dell'
      },
      'dq' : [ ],
      'externalIds' : [ ],
      'keywords' : [
        dlName.toLowerCase()
      ],
      'tags' : [ ],
      'type' : 'app.dataload'
    };

  db.app.dataloads.insert(dataload);

  print('DL_ID ' + newDlId.valueOf() + ' for ' + badQCount + ' records');
}

db.getLastError();
" >> $OUT

mongo --quiet testdata $OUT > $LOG 
rm $OUT

DLID=`grep "DL_ID" $LOG | cut -d' ' -f2`
if [ ! -z DLID ]
then
curl -H'Content-Type:application/json' -u 'bill.moor@dell.com:passwordone' "https://dell-prd1dl2-int.ssi-cloud.com/rest/api/dell/app.dataloads/$DLID::postProcess" -d '{"parallelChops": 64} --insecure'
fi
echo "---- Done" >> $LOG
echo "$DLID
" >> $DLIDTXT

