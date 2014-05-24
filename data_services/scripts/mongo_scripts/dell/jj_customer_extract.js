rs.slaveOk();
load('../common/helper.js');

var printContact = function(doc) {
   if (!doc) return;

   var displayName = doc.displayName;
   var uid = id(doc);
   var segment = extValue(doc, 'segment', false);
   var region = extValue(doc, 'region', false);
   var clientTerritory = extValue(doc, 'clientTerritory', false);
   var country = extValue(doc, 'country', false);
   var clientTheatre = extValue(doc, 'clientTheatre', false);
   var buId = extValue(doc, 'buId', false);
   var createdDate = doc.systemProperties.createdOn;
   var dlOn = doc.systemProperties.dlOn;
   
   print(displayName + '\t' + uid + '\t' + segment + '\t' + region + '\t' + clientTerritory + '\t' + country +
           '\t' + clientTheatre + '\t' + buId + '\t' + createdDate + '\t' + dlOn);

}

print('displayName' + '\t' + 'uid' + '\t' + 'segment' + '\t' + 'region' + '\t' + 'clientTerritory' + '\t' + 'country' +
           '\t' + 'clientTheatre' + '\t' + 'buId' + '\t' + 'createdDate' + '\t' + 'dlOn');

var getClause = {
    "displayName": 1,
    "extensions": 1,
    'externalIds.id': 1,
    "systemProperties": 1
    };

var filter = {
"systemProperties.tenant": "dell",
"systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
"type": "core.contact/organization"
};

db.core.contacts.find(filter, getClause).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(printContact);

