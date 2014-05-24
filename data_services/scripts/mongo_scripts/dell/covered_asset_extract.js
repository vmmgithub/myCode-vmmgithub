rs.slaveOk();
load(file);

load('../common/helper.js');

var printAsset = function(doc) {
   if (!doc) return;

   var displayName = doc.displayName;
   var uid = id(doc);
   var shipDate = isoDate(extValue(doc, 'shipDate', false));
   var eosDate = isoDate(extValue(doc, 'eosDate', false));
   var systemType = extValue(doc, 'systemType', false);
   var serialNumber = extValue(doc, 'serialNumber', true);
   var customerRel = getRel(doc, 'customer');
   var customerUID = customerRel && customerRel.id;
   var customerId = customerRel && customerRel.key;
   var productRel = getRel(doc, 'product');
   var productUID = productRel && productRel.id;
   var productId = productRel && productRel.key;

   var affinityOrganization = customerRel && getRel(customerRel, 'affinityOrganization');
   var affinityId = affinityOrganization && extValue(affinityOrganization, 'affinityId', false);
   var affinityPath = affinityOrganization && extValue(affinityOrganization, 'affinityPath', false);

   print(displayName + '\t' + uid + '\t' + serialNumber + '\t' + eosDate + '\t' + systemType + '\t' + shipDate
           + '\t' + customerId + '\t' + customerUID + '\t' + productId + '\t' + productUID
            + '\t' + affinityId + '\t' + affinityPath);

}

print("Covered Asset Name" + '\t' + "Covered Asset UID" + '\t' + "Asset Tag" + '\t' + "EOSL Date" 
  + '\t' + "System Type" + '\t' + "Ship Date" + '\t' + 'Customer Id' + '\t' + "Customer UID" 
  + '\t' + 'Product Id' + '\t' + "Product UID" + '\t' + 'Affinity Id' + '\t' + "Affinity Path" );

var getClause = {
    "displayName": 1,
    "extensions": 1,
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
