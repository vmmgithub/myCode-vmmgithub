var f = {"systemProperties.tenant": "dell", "systemProperties.expiredOn": ISODate("9999-01-01"), "systemProperties.createdOn": {$gt: ISODate('2013-08-22')}};
var printIds = false;

var stats = {};
var incompletes = {};

db.app.dataloads.find(f).forEach(function(d) {
	var name = d.displayName.split(" ")[0];
	var dt = d.systemProperties.createdOn.toLocaleDateString("%Y");
	var file = dt + "  " + name;
	if (!d.inputSummary || !d.inputSummary.collectionSummary || !d.inputSummary.collectionSummary[0]) { 
		incompletes[file] = 1; return; 
	} else {
		if (!stats[file]) stats[file] = {expected: 0, actual: 0, ids: []};
		stats[file].expected += d.inputSummary.collectionSummary[0].numberRecords;	
		stats[file].actual += db[d.inputSummary.collectionSummary[0].collectionName].count({'externalIds.id': d._id.valueOf()});
		stats[file].ids.push(d._id.valueOf());
		stats[file].collectionName = d.inputSummary.collectionSummary[0].collectionName;
	}
	
});

for (var file in stats) {
	if (stats[file].actual == stats[file].expected) {
		print("SUCCESS " + file + " loaded " + stats[file].expected + " records, and actually loaded " + stats[file].actual);
	} else {
		print("ERROR " + file + " loaded " + stats[file].expected + " records, and actually loaded " + stats[file].actual);

		if (printIds) {
			stats[file].ids.forEach(function(id) {
                db[stats[file].collectionName].find({'externalIds.id': id}, {'externalIds.id': 1}).forEach(function(d) {
                	print("UID" + "\t" + file + "\t" + d.externalIds[0].id);
                });
			});
        }
	}
}
print("---------------");
for (var file in incompletes) {
  print("INCOMPLETE " + file);
}


