
load(file);
load('./helper.js');

var printAsset = function(doc) {
   if (!doc) return;

   var displayName = doc.displayName;
   var uid = id(doc);
   var customerRel = getRel(doc, 'customer');
   var customerUID = customerRel && customerRel.id;
   var customerId = customerRel && customerRel.key;
   var productRel = getRel(doc, 'product');
   var productUID = productRel && productRel.id;
   var productId = productRel && productRel.key;

    printVals([
        _id(doc),
        id(doc), 
        doc.displayName, 
        customerId, 
        customerUID, 
        productId, 
        productUID,
    ]);

}

printVals([
    'Covered Asset ID', 
    'Covered Asset UID', 
    'Covered Asset Name', 
    'Customer Id', 
    'Customer UID' , 
    'Product Id', 
    'Product UID',
]);

var getClause = {
    "displayName": 1,
    "relationships.product.targets" : 1,
    "relationships.customer.targets" : 1,
    'externalIds.id': 1,
    "systemProperties": 1
    };

keys.forEach(function(item) {
   if (item == '' || !item) return;
   docs = db.app.assets.find({_id: ObjectId(item), "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z")}, getClause).limit(1).hint({_id: 1});
   docs.forEach(printAsset);
});

uids.forEach(function(item) {
   if (item == '' || !item) return;
   docs = db.app.assets.find({'externalIds.id': item, "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z")}, getClause).limit(1).hint({externalIds2: 1});
   docs.forEach(printAsset);
});

db.getLastError();
