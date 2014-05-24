var filter = {"systemProperties.tenant": "dell", "systemProperties.expiredOn": ISODate("9999-01-01"), "type": "core.contact/organization", "relationships.salesRep.targets.key": {$exists: 1}};

db.core.contacts.update(filter, {$unset: {"relationships.salesRep": 1}}, false, true);

db.getLastError();
print("Done removing sales reps");
