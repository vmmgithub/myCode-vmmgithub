var affinityIdIndex = {
	    'systemProperties.tenant' : 1,
	    'systemProperties.expiredOn' : 1,
	    'extensions.tenant.affinityId.value': 1
	}, 
	affinityLevelIndex = {
	    'systemProperties.tenant' : 1,
	    'systemProperties.expiredOn' : 1,
	    'extensions.tenant.affinityLevel.value': 1
	},
	affinityPathIndex = {
	    'extensions.tenant.affinityPath.value': 1
	};

db.app.affinity.orgs.ensureIndex(affinityIdIndex, {name: 'affinityIdIndex'});
db.app.affinity.orgs.ensureIndex(affinityLevelIndex, {name: 'affinityLevelIndex'});
db.app.affinity.orgs.ensureIndex(affinityPathIndex, {name: 'affinityPathIndex'});

var i = 0;
var updated = 0;
var IF_PROJECTION_UPDATE=false; 

db.app.affinity.orgs.find({
	'systemProperties.tenant': 'dell',
	'systemProperties.expiredOn': ISODate('9999-01-01T00:00:00Z'),
	'extensions.tenant.affinityLevel.value': "0"
}).addOption(DBQuery.Option.noTimeout).forEach(function(doc) {
	if (i++ % 1000 == 0) print('[' + ISODate()+ '] Setting done with ' + i + ' for 0 and updated ' + updated);

	var myId = doc.extensions && doc.extensions.tenant && doc.extensions.tenant.affinityId && doc.extensions.tenant.affinityId.value;
	var myPath = ':' + myId + ':';
	var path = {type: 'string', value: myPath};

	if (!doc.extensions.tenant.affinityPath || doc.extensions.tenant.affinityPath.value != myPath) {
		doc.extensions.tenant.affinityPath = path;
		db.app.affinity.orgs.save(doc);
		updated++;

		//Projections
		if (IF_PROJECTION_UPDATE) {
			var shallowFilter = {
				'relationships.affinityOrganization.targets.extensions.tenant.affinityId.value': myId, 
				'relationships.affinityOrganization.targets.extensions.tenant.affinityPath.value': {$ne: myPath}
				};
			var deepFilter = {
				'relationships.customer.targets.relationships.affinityOrganization.targets.extensions.tenant.affinityId.value': myId,
				'relationships.customer.targets.relationships.affinityOrganization.targets.extensions.tenant.affinityPath.value': {$ne: myPath}
			};
			var deepCollections = ['app.opportunities', 'app.offers', 'app.assets'//, 'app.quotes', 'app.bookings', 'app.lineItems'
			];

			db.core.contacts.find(shallowFilter).forEach(function(doc){
				doc.relationships.affinityOrganization.targets[0].extensions.tenant.affinityPath = path;
				db.core.contacts.save(doc);
			});

			deepCollections.forEach(function(coll) {
				db[coll].find(deepFilter).forEach(function(doc){
					doc.relationships.customer.targets[0].relationships.affinityOrganization.targets[0].extensions.tenant.affinityPath = path;
					db[coll].save(doc);
				});
			});
		} else {
			print('{id: "' + myId + '", path: "' + myPath + '"},');
		}

	}
});


var tree = {};
var levels = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20'];
levels.forEach(function(level, levelNum) {
	print('Processing level ------------------------ ' + level + ' and ' + levelNum);
	
	if (!tree[levelNum]) tree[levelNum] = {};
	//if (levelNum >= 2 && tree[levelNum-2]) delete tree[levelNum-2];

	var i = 0;
	var updated = 0;
	db.app.affinity.orgs.find({
		'systemProperties.tenant': 'dell',
		'systemProperties.expiredOn': ISODate('9999-01-01T00:00:00Z'),
		'extensions.tenant.affinityLevel.value': level
	}).addOption(DBQuery.Option.noTimeout).forEach(function(doc) {
		
		var myId = doc.extensions && doc.extensions.tenant && doc.extensions.tenant.affinityId && doc.extensions.tenant.affinityId.value;
		var parent = doc.relationships && doc.relationships.parent && doc.relationships.parent.targets && doc.relationships.parent.targets[0];
		var parentAffId = parent && (parent.id || parent.extensions.tenant.affinityId.value);
		var myPath;

		if (level == '1')
			myPath = ':' + parentAffId + ':' + myId + ':';
		else
			myPath = tree[levelNum-1][parentAffId] + myId + ':';

		tree[levelNum][myId] = myPath;

		if (i++ % 1000 == 0) print('[' + ISODate()+ '] Setting done with ' + level + ' for ' + i + ' and updated ' + updated);

		var path = {type: 'string', value: myPath};
		if (!doc.extensions.tenant.affinityPath || doc.extensions.tenant.affinityPath.value != myPath) {
			doc.extensions.tenant.affinityPath = path;
			db.app.affinity.orgs.save(doc);
			updated++;	

			//Projections
			if (IF_PROJECTION_UPDATE) {
				var shallowFilter = {
					'relationships.affinityOrganization.targets.extensions.tenant.affinityId.value': myId, 
					'relationships.affinityOrganization.targets.extensions.tenant.affinityPath.value': {$ne: myPath}
					};
				var deepFilter = {
					'relationships.customer.targets.relationships.affinityOrganization.targets.extensions.tenant.affinityId.value': myId,
					'relationships.customer.targets.relationships.affinityOrganization.targets.extensions.tenant.affinityPath.value': {$ne: myPath}
				};
				var deepCollections = ['app.opportunities', 'app.offers', 'app.assets'/*, 'app.quotes', 'app.bookings', 'app.lineItems'*/];
				
				db.core.contacts.find(shallowFilter).forEach(function(doc){
					doc.relationships.affinityOrganization.targets[0].extensions.tenant.affinityPath = path;
					db.core.contacts.save(doc);
				});
				
				deepCollections.forEach(function(coll){
					db[coll].find(deepFilter).forEach(function(doc){
						doc.relationships.customer.targets[0].relationships.affinityOrganization.targets[0].extensions.tenant.affinityPath = path;
						db[coll].save(doc);
					});
				});
			} else {
				print('{id: "' + myId + '", path: "' + myPath + '"},');
			}				
		}
	});
});

db.getLastError();
