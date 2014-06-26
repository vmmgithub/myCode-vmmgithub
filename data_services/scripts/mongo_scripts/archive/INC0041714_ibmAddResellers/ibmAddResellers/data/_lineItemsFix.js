var remoteCollName = "ibm/app.lineitems";

var tenants = [ 'avnet1', 'avnet2' ];

tenants.forEach(function(tenant) {
    var a1 = db.core.metadata.collections.findOne({
        "systemProperties.tenant":"master",
        'systemProperties.expiredOn': ISODate("9999-01-01T00:00:00Z"),
        "name":"app.lineitems"
    });
    if(!a1) throw new Error('failed to fetch app.lineitems collection from master tenant');
    delete a1._id;  // Remove existing _id to add a new record

    var oldColl = db.core.metadata.collections.findOne({
        "systemProperties.tenant": tenant,
        'systemProperties.expiredOn': ISODate("9999-01-01T00:00:00Z"),
        "name":"app.lineitems"
    });
    if(oldColl && oldColl.remoteCollection == remoteCollName) {
        print('already present remote collection for ' + tenant + ' at core.metadata.collections, id ' + oldColl._id);
    } else {
        print('Adding remote collection for tenant ' + tenant);
        a1.remoteCollection = remoteCollName; // This is a key property which is required for channel tenant
        a1.systemProperties.tenant = tenant;
        a1.systemProperties.createdBy = 're.sell@' + tenant + '.com';
        db.core.metadata.collections.save(a1);
    }
});
