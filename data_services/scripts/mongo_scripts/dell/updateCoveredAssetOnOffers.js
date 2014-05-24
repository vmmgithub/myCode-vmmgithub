var filter =
{
	"systemProperties.tenant" : "dell",
	"systemProperties.expiredOn" : ISODate("9999-01-01T00:00:00Z"),
	"relationships.covered.targets.key" : {
		"$exists" : 0
	},
	"relationships.predecessor.targets.relationships.covered.targets.key" : {
		"$exists" : 1
	}
};
var i = 0;
var updateCovered = function(doc) { 
	if (i++ %100 == 0) print('Completed ' + i);
	var cov = doc.relationships.predecessor.targets[0].relationships.covered.targets; 
	if (cov) db.app.lineitems.update({_id: doc._id}, {$set: {'relationships.covered.targets': cov}}); 
};

db.app.lineitems.find(filter).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(updateCovered);

db.getLastError();

/*
var filter =
{
        "systemProperties.tenant" : "dell",
        "systemProperties.expiredOn" : ISODate("9999-01-01T00:00:00Z"),
        "relationships.covered.targets.key" : {
                "$exists" : 0
        },
        "relationships.predecessor.targets.relationships.covered.targets.key" : {
                "$exists" : 0
        }
};
var i = 0;
var lkp = {};
var updateCovered = function(doc) {
        var cov = doc.relationships && doc.relationships.predecessor && doc.relationships.predecessor.targets[0] && doc.relationships.predecessor.targets[0].key;
        lkp[cov] = 1;
};

db.app.offers.find(filter).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(updateCovered);
printjson(lkp);
db.getLastError();
*/
