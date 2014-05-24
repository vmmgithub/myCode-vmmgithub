var extValue = function (doc, name, isMaster) {
  var e = isMaster ? 'master': 'tenant';
  if (doc && doc.extensions && doc.extensions[e] && doc.extensions[e][name] && doc.extensions[e][name].value) {
    if (doc.extensions[e][name].type != 'date')
      return (doc.extensions[e][name].value.displayName || doc.extensions[e][name].value.name || doc.extensions[e][name].value);
    else
      return isoDate(doc.extensions[e][name].value);
  } else {
    return '';
  }
};

var getRel = function (doc, name) {
  return (doc && doc.relationships && doc.relationships[name] && doc.relationships[name].targets && doc.relationships[name].targets[0]);
};

var printVals = function(arr) {
        var str = "";
        arr.forEach(function(a) {
                str += a + "\t";
        });
        print (str);
};

var filter = {
    'systemProperties.tenant': 'dell',
    'systemProperties.expiredOn': ISODate('9999-01-01'),
    'type': 'core.contact/organization',
    'extensions.tenant.IBReport.value': {$exists: 1}
};

printVals(['UID', 'NAME', 'CUSTOMERNUMBER', 'BUID']);
db.core.contacts.find(filter, {displayName: 1, 'extensions.tenant.customerNumber': 1, 'extensions.tenant.buId': 1}).addOption(DBQuery.Option.noTimeout).readPref('secondary').forEach(function(doc) {
	printVals([doc._id.valueOf(), doc.displayName, extValue(doc, 'customerNumber', false),  extValue(doc, 'buId', false), ]);
});
