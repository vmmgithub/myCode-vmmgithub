load('./noRep.js');

var getUID = function(doc, type) {
  type = type || 'UID';
  var id;

  if (doc && doc.externalIds) {
    doc.externalIds.forEach(function(xid) {
       if (xid.schemeId.name == type) id = xid.id;
    });
  }
  return id;
};

var i = 0;
var updated = 0;

ids.forEach(function(id){
  if (i++ % 1000 == 0) print('[' + ISODate()+ '] Setting done with for ' + i + ' records and updated ' + updated);

  db.app.affinity.orgs.find({'externalIds.id': id, 'systemProperties.expiredOn' : ISODate('9999-01-01T00:00:00Z'), 'relationships.salesRep.targets.key': {$exists: 1}}).forEach(function(doc) {
    var affinityPath = doc.extensions && doc.extensions.tenant && doc.extensions.tenant.affinityPath && doc.extensions.tenant.affinityPath.value;
    print(doc._id.valueOf() + '\t' + getUID(doc) + '\t' + affinityPath);
    delete doc.relationships.salesRep;
    db.app.affinity.orgs.save(doc);
    updated++;
  });
});
