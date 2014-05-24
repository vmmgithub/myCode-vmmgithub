rs.slaveOk();
load('../common/helper.js');

// 1. Find the tags

var filter = {
    "systemProperties.tenant": "dell",
    "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
    "type": "core.contact/organization",
"extensions.tenant.buId.value": buId,
//    "extensions.tenant.IBReport.value": {$exists: 1},
};

// 2. Find the IB Reports of Customers and print them

//printVals(['UID', 'TYPE', 'BUID']);
//db.core.contacts.find(filter, {displayName: 1, 'extensions.tenant.customerNumber': 1, 'extensions.tenant.buId': 1}).addOption(DBQuery.Option.noTimeout).readPref('secondary').forEach(function(doc) {
//printVals([doc._id.valueOf(), doc.type,  extValue(doc, 'buId', false), ]);
//	printVals([doc._id.valueOf(), doc.displayName, extValue(doc, 'customerNumber', false), extValue(doc, 'buId', false), ]);

printVals(['UID', 'NAME', 'CUSTOMERNUMBER', 'BUID', 'IBREPORT']);
db.core.contacts.find(filter, {"displayName": 1, "extensions.tenant.customerNumber": 1, "extensions.tenant.buId": 1, "extensions.tenant.IBReport": 1}).addOption(DBQuery.Option.noTimeout).readPref('secondary').forEach(function (doc) {
	printVals([doc._id.valueOf(), doc.displayName, extValue(doc, 'customerNumber', false), extValue(doc, 'buId', false), extValue(doc, 'IBReport', false), ]);

});
print("DONE");
