var i = 0;
var tenant="juniper";
var coll = "app.opportunities";

var getLookup = function(name, core, group) {
        var coll = core ? "core.lookups": "app.lookups";
	var filter = {"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), displayName: name};
	if (group) filter.group = group;

        var lkp = db[coll].findOne(filter);
        if (lkp) {
                var res = {name: lkp.name, displayName: lkp.displayName, type: lkp.type, key: lkp._id.valueOf()};
                return res;
        }
};

var defClientTheatre = getLookup('NALA', false, "ClientTheatre");
var defTerritory = getLookup('WEST', false, "ClientTerritory");
var defClientRegion = getLookup('North America', false, "ClientRegion");
var defCountry = getLookup('United States', true, "Country");

printjson([defClientTheatre, defTerritory, defClientRegion, defCountry]);

db.app.opportunities.update({"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), 'extensions.master.clientTheatre.value.name': {$exists: false}}, {$set: {'extensions.master.clientTheatre': {type: 'lookup', value: defClientTheatre}}}, false, true);
db.app.opportunities.update({"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), 'extensions.master.clientTerritory.value.name': {$exists: false}}, {$set: {'extensions.master.clientTerritory': {type: 'lookup', value: defTerritory}}}, false, true);
db.app.opportunities.update({"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), 'extensions.master.clientRegion.value.name': {$exists: false}}, {$set: {'extensions.master.clientRegion': {type: 'lookup', value: defClientRegion}}}, false, true);
db.app.opportunities.update({"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), 'extensions.master.defCountry.value.name': {$exists: false}}, {$set: {'extensions.master.defCountry': {type: 'lookup', value: defCountry}}}, false, true);

