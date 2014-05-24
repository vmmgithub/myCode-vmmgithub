rs.slaveOk();
load(file);

load('../common/helper.js');

var printContact = function(doc) {
   if (!doc) return;

   var displayName = doc.displayName;
   var uid = id(doc);
   var clientTerritory = extValue(doc, 'clientTerritory', false);
   var segment = extValue(doc, 'segment', false);
   var country = extValue(doc, 'country', false);
   var region = extValue(doc, 'region', false);
   var customerNumber = extValue(doc, 'customerNumber', false);
   var affinityOrganization = getRel(doc, 'affinityOrganization');
   var affinityUID = affinityOrganization && affinityOrganization.id;
   var affinityId = affinityOrganization && affinityOrganization.key;
   var affinityPath = affinityOrganization && extValue(affinityOrganization, 'affinityPath', false);
   var l0affinity = affinityOrganization && extValue(affinityOrganization, 'l0AffinityId', false);
   
   print(displayName + '\t' + uid + '\t' + clientTerritory + '\t' + country + '\t' + segment + '\t' + affinityId +
           '\t' + affinityUID + '\t' + affinityPath + '\t' + l0affinity  + '\t' + customerNumber);

}

print("Customer Name" + '\t' + "Customer UID" + '\t' + "Territory" + '\t' + "Country" + '\t' + "Segment" +
                + '\t' + "Affinity Id" + '\t' + "Affinity UID" + '\t' + 'Affinity Path' + '\t' + 'L0 Affinity Id'  + '\t' + 'customerNumber');

var getClause = {
    "displayName": 1,
    "extensions": 1,
    "relationships.affinityOrganization.targets" : 1,
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
