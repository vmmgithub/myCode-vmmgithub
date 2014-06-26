load('../common/helper.js');

var f = {
	"systemProperties.tenant" : "ibm",
	"systemProperties.expiredOn" : ISODate("9999-01-01T00:00:00Z"),
	"relationships.covered.targets.key" : {
		"$exists" : false
	},
	"type" : "app.asset/service",
	"endDate" : {
		"$gt" : ISODate("2013-01-01T00:00:00Z"),
		"$lt" : ISODate("2015-12-31T00:00:00Z")
	}
};

printVals(['_ID', 'DISPLAYNAME', 'ENDDATE']);

db.app.assets.find(f).forEach(function(a) {
printVals([_id(a), a.displayName, isoDate(a.endDate)]);
});
