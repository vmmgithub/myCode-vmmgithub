db[coll].find({
  "systemProperties.tenant": "dell",
  "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
  "extensions.tenant.country.value.name": countryCode,
  "type": type
}, {
  _id: 1,
  "externalIds": 1,
  "type" : 1,
  "extensions.tenant.country.value.name" : 1
})
.addOption(DBQuery.Option.noTimeout)
.readPref('secondary')
.forEach(function(r) {
  var id;
  r.externalIds.forEach(function(i) {
    if(i.schemeId.name == "UID") {
      id = i.id;
    }
  });
  print(r._id.valueOf() + '\t' + id);
});
