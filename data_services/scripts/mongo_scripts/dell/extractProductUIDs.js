load('../common/helper.js');

printVals([ 'ID', 'uid', 'sku', 'category', 'serviceClass', 'rank', 'portfolio', ]);

db[coll].find({
  "systemProperties.tenant": "dell",
  "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
  "type": type
}, {
  _id: 1,
  "externalIds": 1,
  "extensions": 1,
})
.addOption(DBQuery.Option.noTimeout)
.readPref('secondary')
//.limit(100)
.forEach(function(r) {
  var id;
  r.externalIds.forEach(function(i) {
    if(i.schemeId.name == "UID") {
      id = i.id;
    }
  });
        printVals([r._id.valueOf(), id, extValue(r, 'sku', true), extValue(r, 'category', true), extValue(r, 'serviceClass', false), extValue(r, 'rank', false), extValue(r, 'portfolio', false)
        ]);
});
