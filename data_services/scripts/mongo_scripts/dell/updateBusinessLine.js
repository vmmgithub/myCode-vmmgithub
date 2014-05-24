var i = 0;
load(file);

var enterprise = {
	"type" : "lookup",
	"value" : {
		"name" : "enterprise",
		"type" : "app.lookup",
		"displayName" : "Enterprise Products",
		"key" : "50ac01a51f9c0c0000000bac"
	}
};

var client = {
	"type" : "lookup",
        "value" : {
                "name" : "client",
                "type" : "app.lookup",
                "displayName" : "Client Products",
                "key" : "50ac01a51f9c0c0000000bab"
        }
};

var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$");

values.forEach(function(v) {
	if (i++ % 100 == 0) print('[' + ISODate()+ '] Setting done with for ' + i + ' records ');
	if (!v.uid || v.uid == 'undefined' || !checkForHexRegExp.test(v.uid)) return;

	var businessLine = (v.value == 'Enterprise' || v.value == "Enterprise Products") ? enterprise: client;

	db.app.opportunities.update({'_id': ObjectId(v.uid), "systemProperties.expiredOn" :  ISODate("9999-01-01T00:00:00Z")}, {$set: {'extensions.master.businessLine': businessLine }});
});

print("Done " + i);
