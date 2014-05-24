rs.slaveOk();
load('../common/helper.js');

var filter = {
"systemProperties.tenant": "dell",
"systemProperties.expiredOn": ISODate("9999-01-01"),
"type" :"app.asset/covered",
    "extensions.tenant.buId.value" : buid,
};

print("Id" + "\t" + "_id" + "\t" + "Name" );

db.app.assets.find(filter)
.readPref('secondary')
.limit(10)
.addOption(DBQuery.Option.noTimeout)
.forEach(function(d) {
var name = d.displayName;
var id = d.externalIds[0].id;

print(id + "\t" + d._id.valueOf() + "\t" + name);

});
