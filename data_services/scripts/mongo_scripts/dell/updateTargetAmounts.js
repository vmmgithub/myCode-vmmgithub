var i = 0;
load(file);
load('../common/helper.js');

var code = {
	"key" : "50ac01a21f9c0c00000007d8",
	"displayName" : "USD",
	"type" : "core.lookup",
	"name" : "usd"
};

var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$");

values.forEach(function(v) {
	if (i++ % 100 == 0) print('[' + ISODate()+ '] Setting done with for ' + i + ' records ');
	if (!v.uid || v.uid == 'undefined' || !checkForHexRegExp.test(v.uid)) return;
	var amount = {code: code, type: "core.currency", amount: v.value, normalizedAmount: {code: code, amount: toFixed(v.value), convertedOn: ISODate()}};

	db[coll].update({'_id': ObjectId(v.uid), "systemProperties.expiredOn" :  ISODate("9999-01-01T00:00:00Z")}, {$set: {'targetAmount': amount }});

	if (coll == "app.opportunities") 
	db[coll].update({'_id': ObjectId(v.uid), "systemProperties.expiredOn" :  ISODate("9999-01-01T00:00:00Z"), 'flows.salesStages.state.name': {$in: ['notContacted', 'quoteRequested']}}, {$set: {'amount': amount }});
});

print("Done " + i);
