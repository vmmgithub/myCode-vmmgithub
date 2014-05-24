var i = 0;
load(file);
load('../common/helper.js');
var tenant="dell";
var coll = "app.opportunities";

var cache = {};

var getLookup = function(name, core) {
        var coll = core ? "core.lookups": "app.lookups";
	if (cache[coll] && cache[coll].name) return cache[coll].name;

        var lkp = db[coll].findOne({"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), displayName: name});
        if (lkp) { 
		var res = {name: lkp.name, displayName: lkp.displayName, type: lkp.type, key: lkp._id.valueOf()};
		if (!cache[coll]) cache[coll] = {};
		cache[coll].name = res;
                return res;
	}
};

var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$");

values.forEach(function(v) {
	if (i++ % 100 == 0) print('[' + ISODate()+ '] Setting done with for ' + i + ' records ');

	if (!v.uid || v.uid == 'undefined' || !checkForHexRegExp.test(v.uid)) return;

	var terr = getLookup(v.value);
	if (!terr) return;

	db[coll].update({'_id': ObjectId(v.uid), "systemProperties.expiredOn" :  ISODate("9999-01-01T00:00:00Z")}, {$set: {'extensions.master.clientTerritory': {type: 'lookup', value: terr} }});
});

print("Done " + i);
