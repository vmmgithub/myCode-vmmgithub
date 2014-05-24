load(file);
load('./helper.js');

var printContact = function(doc) {
   if (!doc) return;

    printVals([
        _id(doc),
        id(doc), 
        doc.displayName, 
        extValue(doc, 'country', true), 
        extValue(doc, 'clientTerritory', true), 
        extValue(doc, 'clientTheater', true), 
        extValue(doc, 'clientRegion', true), 
    ]);

}

printVals([
    'Customer ID', 
    'Customer UID', 
    'Customer Name', 
    'Country', 
    'Territory' , 
    'Theater', 
    'Region',
]);

var getClause = {
    "displayName": 1,
    "extensions": 1,
    'externalIds.id': 1,
    "systemProperties": 1
    };

keys.forEach(function(item) {
   if (item == '' || !item) return;
   docs = db.core.contacts.find({_id: ObjectId(item), "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z")}, getClause).limit(1).hint({_id: 1});
   docs.forEach(printContact);
});

uids.forEach(function(item) {
   if (item == '' || !item) return;
   docs = db.core.contacts.find({'externalIds.id': item, "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z")}, getClause).limit(1).hint({externalIds2: 1});
   docs.forEach(printContact);
});

db.getLastError();
