load('./affinityPaths.js');
var i =0;
var updated=0;
paths.forEach(function(doc) {
	if (i++ % 1000 == 0) print('[' + ISODate()+ '] Setting done with ' + i + ' for 0 and updated ' + updated);
if (i < 92001) return;

	var myId = doc.id;
	var myPath = doc.path;
	var path = {type: 'string', value: myPath};

		var shallowFilter = {
			'relationships.affinityOrganization.targets.extensions.tenant.affinityId.value': myId, 
			'relationships.affinityOrganization.targets.extensions.tenant.affinityPath.value': {$ne: myPath}
			};
		var deepFilter = {
			'relationships.customer.targets.relationships.affinityOrganization.targets.extensions.tenant.affinityId.value': myId,
			'relationships.customer.targets.relationships.affinityOrganization.targets.extensions.tenant.affinityPath.value': {$ne: myPath}
		};
		var deepCollections = [//'app.opportunities', 
		//	'app.offers', 
			'app.assets'
			/*, 'app.quotes', 'app.bookings', 'app.lineItems'*/];

		deepCollections.forEach(function(coll) {
			db[coll].find(deepFilter).addOption(DBQuery.Option.noTimeout).forEach(function(doc){
				doc.relationships.customer.targets[0].relationships.affinityOrganization.targets[0].extensions.tenant.affinityPath = path;
				db[coll].save(doc);
updated++;
			});
		});
});

db.getLastError();
