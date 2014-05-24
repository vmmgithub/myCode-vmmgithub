// Clean db
load('./helper.js');

var expirationDate = ISODate();

var cols = ["app.tasks", "app.offers", "app.opportunities", "app.quotes", "app.bookings",
    "app.lineitems", "app.assets", "core.notes", "core.addresses", "app.products"
];

/////////////////////////////////////////////////////
// Helper function to remove the transactional records
var cleanCollections = function(ten) {
    var filter = {
        "systemProperties.tenant": ten,
        "systemProperties.expiredOn": ISODate("9999-01-01")
    };

    cols.forEach(function (c) {
        print('[' + ten + '] ' + c + ' before ' + db[c].count(filter) + ' records ');
        if (!mock) 
           db[c].update(filter, {$set: {"systemProperties.expiredOn": expirationDate}}, false, true);
        print('[' + ten + '] ' + c + ' after ' + db[c].count(filter) + ' records ');
    });
};

/////////////////////////////////////////////////////
// Helper function to remove the people records
var cleanPeople = function(ten) {
    //Clean out junk person records
    var filter = {
        "systemProperties.tenant": ten,
        "systemProperties.expiredOn": ISODate("9999-01-01"),
        type: "core.contact/person",
        membership: {$exists: 0}, 
        //membership: {$not: /^./ },
    };
    var c = 'core.contacts';
    print('[' + ten + '] ' + 'people' + ' before ' + db[c].count(filter) + ' records ');
    if (!mock) {
      db[c].update(filter, {$set: {"systemProperties.expiredOn": expirationDate}}, false, true);
      db.core.contacts.update({'systemProperties.tenant': ten,  type: 'core.contact/person', membership: {$exists: 1}}, {$set: {'systemProperties.expiredOn': ISODate('9999-01-01')}}, false, true);
    }
    print('[' + ten + '] ' + 'people' + ' after ' + db[c].count(filter) + ' records ');
}

var getSubTenantKeys = function(ten) {
    var subKeys = [];
    db.core.tenants.find({"relationships.channelMaster.targets.name": ten, "systemProperties.expiredOn": ISODate("9999-01-01")}).forEach(function(t) {
        var k = getRelKey(t, 'owner');
        if (k) subKeys.push(ObjectId(k));
    });
    return subKeys;
}

/////////////////////////////////////////////////////
// Helper function to remove the organization records
var cleanOrgs = function(ten, tenKey) {

    var c = 'core.contacts';
    var subKeys = getSubTenantKeys(ten);
    subKeys.push(ObjectId(tenKey));

    var filter = {
        "systemProperties.tenant": ten,
        "systemProperties.expiredOn": ISODate("9999-01-01"),
        "type" : "core.contact/organization",
        _id: {
            $nin: subKeys
        },
    };

    print('[' + ten + '] ' + 'Excluding delete of sub tenant org records ' + subKeys.length);
    print('[' + ten + '] ' + 'organizations' + ' before ' + db[c].count(filter) + ' records ');
    if (!mock) db[c].update(filter, {$set: {"systemProperties.expiredOn": expirationDate}}, false, true);
    print('[' + ten + '] ' + 'organizations' + ' after ' + db[c].count(filter) + ' records ');
}

/////////////////////////////////////////////////////
// Invoke the individual collections
var cleanTenant = function(ten) {
    var t = db.core.tenants.findOne({name: ten, 'systemProperties.expiredOn': ISODate('9999-01-01')});
    var tenKey = getRelKey(t, 'owner');

    if (!t || !tenKey) {
        print("Exiting as we might have a config issue with the tenant  ... " + ten );
        return;
    }

    cleanCollections(ten);
    cleanPeople(ten);
    cleanOrgs(ten, tenKey);

}

/////////////////////////////////////////////////////
// Money maker to clean the system
/////////////////////////////////////////////////////

print('[' + tenant + '] removing all transactional data with expirationDate as ' + expirationDate);

//1: Delete the partner objects first
db.core.tenants.find({"relationships.channelMaster.targets.name": tenant}, {name: 1, 'relationships.owner.targets': 1}).forEach(function(t) {
    cleanTenant(t.name);
});

//2: Delete the OEM objects next
cleanTenant(tenant);

// Clean db
load('./helper.js');

var expirationDate = ISODate();

var cols = ["app.tasks", "app.offers", "app.opportunities", "app.quotes", "app.bookings",
    "app.lineitems", "app.assets", "core.notes", "core.addresses", "app.products"
];

/////////////////////////////////////////////////////
// Helper function to remove the transactional records
var cleanCollections = function(ten) {
    var filter = {
        "systemProperties.tenant": ten,
        "systemProperties.expiredOn": ISODate("9999-01-01")
    };

    cols.forEach(function (c) {
        print('[' + ten + '] ' + c + ' before ' + db[c].count(filter) + ' records ');
        if (!mock) 
           db[c].update(filter, {$set: {"systemProperties.expiredOn": expirationDate}}, false, true);
        print('[' + ten + '] ' + c + ' after ' + db[c].count(filter) + ' records ');
    });
};

/////////////////////////////////////////////////////
// Helper function to remove the people records
var cleanPeople = function(ten) {
    //Clean out junk person records
    var filter = {
        "systemProperties.tenant": ten,
        "systemProperties.expiredOn": ISODate("9999-01-01"),
        type: "core.contact/person",
        membership: {$exists: 0}, 
        //membership: {$not: /^./ },
    };
    var c = 'core.contacts';
    print('[' + ten + '] ' + 'people' + ' before ' + db[c].count(filter) + ' records ');
    if (!mock) {
      db[c].update(filter, {$set: {"systemProperties.expiredOn": expirationDate}}, false, true);
      db.core.contacts.update({'systemProperties.tenant': ten,  type: 'core.contact/person', membership: {$exists: 1}}, {$set: {'systemProperties.expiredOn': ISODate('9999-01-01')}}, false, true);
    }
    print('[' + ten + '] ' + 'people' + ' after ' + db[c].count(filter) + ' records ');
}

var getSubTenantKeys = function(ten) {
    var subKeys = [];
    db.core.tenants.find({"relationships.channelMaster.targets.name": ten, "systemProperties.expiredOn": ISODate("9999-01-01")}).forEach(function(t) {
        var k = getRelKey(t, 'owner');
        if (k) subKeys.push(ObjectId(k));
    });
    return subKeys;
}

/////////////////////////////////////////////////////
// Helper function to remove the organization records
var cleanOrgs = function(ten, tenKey) {

    var c = 'core.contacts';
    var subKeys = getSubTenantKeys(ten);
    subKeys.push(ObjectId(tenKey));

    var filter = {
        "systemProperties.tenant": ten,
        "systemProperties.expiredOn": ISODate("9999-01-01"),
        "type" : "core.contact/organization",
        _id: {
            $nin: subKeys
        },
    };

    print('[' + ten + '] ' + 'Excluding delete of sub tenant org records ' + subKeys.length);
    print('[' + ten + '] ' + 'organizations' + ' before ' + db[c].count(filter) + ' records ');
    if (!mock) db[c].update(filter, {$set: {"systemProperties.expiredOn": expirationDate}}, false, true);
    print('[' + ten + '] ' + 'organizations' + ' after ' + db[c].count(filter) + ' records ');
}

/////////////////////////////////////////////////////
// Invoke the individual collections
var cleanTenant = function(ten) {
    var t = db.core.tenants.findOne({name: ten, 'systemProperties.expiredOn': ISODate('9999-01-01')});
    var tenKey = getRelKey(t, 'owner');

    if (!t || !tenKey) {
        print("Exiting as we might have a config issue with the tenant  ... " + ten );
        return;
    }

    cleanCollections(ten);
    cleanPeople(ten);
    cleanOrgs(ten, tenKey);

}

/////////////////////////////////////////////////////
// Money maker to clean the system
/////////////////////////////////////////////////////

print('[' + tenant + '] removing all transactional data with expirationDate as ' + expirationDate);

//1: Delete the partner objects first
db.core.tenants.find({"relationships.channelMaster.targets.name": tenant}, {name: 1, 'relationships.owner.targets': 1}).forEach(function(t) {
    cleanTenant(t.name);
});

//2: Delete the OEM objects next
cleanTenant(tenant);

