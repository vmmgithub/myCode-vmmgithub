var colls = ['app.opportunities', 'app.quotes', 'app.assets', 'app.offers'];

colls.forEach(function(coll){
	db[coll].find({'systemProperties.tenant': 'siemens','systemProperties.expiredOn': ISODate('9999-01-01'), 
		'relationships.reseller.targets.displayName': {$exists: false}, 'relationships.reseller.targets.key': {$exists: true}}).forEach(function(opp){
		print(coll + ' ' + opp._id.valueOf() + ' ' + opp.displayName + ' had reseller key of ' + opp.relationships.reseller.targets[0].key);
		db[coll].update({_id: opp._id}, {$unset: {'relationships.reseller': 1}});
	});

	db[coll].find({'systemProperties.tenant': 'siemens','systemProperties.expiredOn': ISODate('9999-01-01'), 
		'relationships.primaryReseller.targets.displayName': {$exists: false}, 'relationships.primaryReseller.targets.key': {$exists: true}}).forEach(function(opp){
		print(coll + ' ' + opp._id.valueOf() + ' ' + opp.displayName + ' had primaryReseller key of ' + opp.relationships.primaryReseller.targets[0].key);
		db[coll].update({_id: opp._id}, {$unset: {'relationships.primaryReseller': 1}});
	});

});

