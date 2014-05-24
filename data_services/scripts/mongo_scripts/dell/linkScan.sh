#!/bin/bash

COLL="$1"
BUID="$2"
TYPE="$3"
IN="$1.$2"
OUT="$IN.link.js"
LOG="$IN.link.log"

if [[ -f $OUT ]]
then
rm $OUT
fi

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

var getCollectionName = function(modelName) {
  var coll = undefined;
  switch (modelName) {
    case 'core.contact/organization':
    case 'core.contact/person':
      coll = 'core.contacts';
      break;
    case 'app.asset/service':
    case 'app.asset/covered':
      coll = 'app.assets';
      break;
    case 'app.product/service':
    case 'app.product/covered':
      coll = 'app.products';
      break;
    case 'app.affinity.org':
      coll = 'app.affinity.orgs';
      break;
    case 'core.address':
      coll = 'core.addresses';
      break;
  }
  return coll;
}

var checkBrokenLink = function(obj, relName) {
  if (obj && obj.relationships && obj.relationships[relName] && obj.relationships[relName].targets && 
    obj.relationships[relName].targets[0] && obj.relationships[relName].targets[0].id) {
      var coll = getCollectionName(obj.relationships[relName].targets[0].type);
      if (coll) {
        var o = db[coll].findOne({
          'externalIds.id': obj.relationships[relName].targets[0].id, 
          'systemProperties.expiredOn' : ISODate('9999-01-01T00:00:00Z')
        });

        return {found: o, id: obj.relationships[relName].targets[0].id};
      }
  }
};

var filter = {
  'systemProperties.tenant' : 'dell',
  'systemProperties.expiredOn' : ISODate('9999-01-01T00:00:00Z'),
  'type': '$TYPE',
  'extensions.tenant.buId.value': '$BUID',
};

var cursor = db.$COLL.find(filter);
var errors = [];
var i = 0;
while (cursor.hasNext()) {
  var doc = cursor.next();
  if (i++ % 1000 == 0) print('[' + ISODate() + '] Processed ' + i +' records ');

  var unresolvedLinks = false;
  var links = '';
  var uid = getUID(doc);

  for (relName in doc.relationships) {
    var xid = checkBrokenLink(doc, relName);
    if (xid) {
      unresolvedLinks = true;
      links =  links + '\t' + relName + ':' + xid.found + ':' + xid.id;
    }
  }

  if (unresolvedLinks) {
    print('LINKISSUE', '\t', uid, '\t', links)
  }
}

db.getLastError();
" > $OUT

mongo --quiet testdata $OUT > $LOG 
rm $OUT

