var tenant = "juniper";
var mock = true;

var getLookupWithValue = function(tenant, name) {
        var lkp = db.app.lookups.findOne({"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), displayName: name});
        if (lkp) 
            return {name: lkp.name, displayName: lkp.displayName, type: lkp.type, key: lkp._id.valueOf(), value: lkp.value};
};

var changes = [{from: "FY13", to: "FY13Q3"}];
var colls = ["app.opportunities", "app.offers"];

colls.forEach(function(coll) {
	changes.forEach(function(change) {
		var filter = {'systemProperties.tenant': tenant, 'systemProperties.expiredOn': ISODate('9999-01-01'), 'extensions.master.targetPeriod.value.name': change.from};
		var to = getLookupWithValue(tenant, change.to);

		if (!to) {
			print('Unable to find the target period' + change.to);
			return;
		}
		
		print("Before count for " + coll + ' with ' + change.from + ' is ' + db[coll].count(filter));
		if(!mock) db[coll].update(filter, {$set: {'extensions.master.targetPeriod': {type: 'lookup', value: to}}}, false, true);
		print("After count for " + coll + ' with ' + change.from + ' is ' + db[coll].count(filter));
	});
});
