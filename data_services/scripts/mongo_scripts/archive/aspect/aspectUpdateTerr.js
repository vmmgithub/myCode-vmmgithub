load('./aspect2.js')
var tenant = "aspect";
var cache = {};
var getLookup = function(name, core) {
	if (cache[name]) return cache[name];

    var coll = core ? "core.lookups": "app.lookups";
    var lkp = db[coll].findOne({"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), displayName: name});
    if (!lkp) {
        return lkp;
    } else {
    	cache[name] = {name: lkp.name, displayName: lkp.displayName, type: lkp.type, key: lkp._id.valueOf()};
        return cache[name];
    }
};

var getRel = function (doc, name) {
  return (doc && doc.relationships && doc.relationships[name] && doc.relationships[name].targets && doc.relationships[name].targets[0]);
};

var i = 0;
ass.forEach(function(assDet) {
 if (i++ % 1000 == 0) print('[' + ISODate() + '] Processed ' + i +' records');
	var l = getLookup(assDet.val, false);
	if (!l) return;

	db.app.assets.update({'systemProperties.tenant': tenant, 'systemProperties.expiredOn': ISODate('9999-01-01'), 'externalIds.id': assDet.udi}, 
		{$set: {'extensions.master.clientTerritory.value': l}}, false, true);
});
