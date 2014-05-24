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
	}
	return coll;
}

var checkBrokenLink = function(obj, relName) {
  if (obj && obj.relationships && obj.relationships[relName] && obj.relationships[relName].targets && 
    obj.relationships[relName].targets[0] && obj.relationships[relName].targets[0].id) {
        var coll = getCollectionName(obj.relationships[relName].targets[0].type);
	    var o;
	    if (coll) {
	    	o = db[coll].findOne({
	    		'externalIds.id': obj.relationships[relName].targets[0].id, 
	    		"systemProperties.expiredOn" : ISODate("9999-01-01T00:00:00Z")
	    	});

	    	return {found: o, id: obj.relationships[relName].targets[0].id};
	    }
  }
};

var filter = {
	"systemProperties.tenant" : "dell",
	"systemProperties.expiredOn" : ISODate("9999-01-01T00:00:00Z"),
	"type": 'app.asset/service',
	'extensions.tenant.buId.value': '909',
	//'relationships.customer.targets.extensions.tenant.buId.value': '909',
	'extensions.tenant.primary.value': true,
	associatedOpportunity: false,
	endDate: { '$gte': ISODate('2013-05-04'), '$lt': ISODate('2013-08-03')}
};

var cursor = db.app.assets.find(filter);
var errors = [];
while (cursor.hasNext()) {
  var doc = cursor.next();

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
