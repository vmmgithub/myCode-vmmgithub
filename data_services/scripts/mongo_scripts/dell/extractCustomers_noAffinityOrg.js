rs.slaveOk();

load('../common/helper.js');

var printContact = function(doc) {
   if (!doc) return;

   var displayName = doc.displayName;
   var contactID = doc._id;
   var uid = id(doc);
   var clientTerritory = extValue(doc, 'clientTerritory', false);
   var segment = extValue(doc, 'segment', false);
   var country = extValue(doc, 'country', false);
   var region = extValue(doc, 'region', false);
   var customerNumber = extValue(doc, 'customerNumber', false);
   var affinityOrganization = getRel(doc, 'affinityOrganization');
   var affinityUID = affinityOrganization && affinityOrganization.id;
   var affinityId = affinityOrganization && affinityOrganization.key;
   var l0affinity = extValue(doc, 'l0AffinityId', false);
   var affinityLevel = extValue(doc, 'affinityLevel', false);

printVals([contactID, displayName, uid, clientTerritory, country, segment, affinityUID, affinityId, l0affinity, affinityLevel, customerNumber, ]);
}


printVals([
        'ID', 'Customer Name', 'Customer UID', 'Territory', 'Country', 'Segment', 'Affinity UID', 'Affinity Id', 'L0 Affinity Id', 'Affinity Level', 'CustomerNumber',
]);

var getClause = {
    "displayName": 1,
    "extensions": 1,
    "relationships.affinityOrganization.targets" : 1,
    'externalIds.id': 1,
    "systemProperties": 1
    };

var filter = {
"systemProperties.tenant": "dell",
"systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
"type": "core.contact/organization",
//"extensions.tenant.customerNumber.value": "SE2287183",
"relationships.affinityOrganization.targets.id": "$delete",
//"_id": ObjectId("511e64f7e8792efa192b78a4")
};

db.core.contacts.find(filter, getClause).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(printContact);

