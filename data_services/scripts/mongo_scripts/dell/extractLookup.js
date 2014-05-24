load('../common/helper.js');

printVals([ 'lookupGroup', 'ID', 'displayName', 'name', 'displayPosition', 'value' ]);

db[coll].find({
  "systemProperties.tenant": "dell",
  "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
  "type": type
}, {
  _id: 1,
  "externalIds": 1,
  "displayName": 1,
  "group": 1,
  "name": 1,
  "displayPosition": 1,
  "value": 1,
})
.addOption(DBQuery.Option.noTimeout)
.readPref('secondary')
//.limit(10)
.forEach(function(r) {
        printVals([r.group, r._id.valueOf(), r.displayName, r.name, r.displayPosition, r.value
        ]);
});
