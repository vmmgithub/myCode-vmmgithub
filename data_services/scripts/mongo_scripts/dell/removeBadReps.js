load('./affPaths.js');
var i = 0;
var updated = 0;

affs.forEach(function(aff) {
  if (i++ % 1000 == 0) print('[' + ISODate()+ '] Setting done with for ' + i + ' records and updated ' + updated);

  var affReg = new RegExp('/^:' + aff + '/');
  var oppfilter = {
      'relationships.customer.targets.relationships.affinityOrganization.targets.extensions.tenant.affinityPath.value': affReg,
      'systemProperties.expiredOn' : ISODate('9999-01-01T00:00:00Z'),
      'relationships.customer.targets.extensions.tenant.buId.value': {$nin: ['2121', '5455', '1212', '2323', '1224', '3131', '3434', '1222', '5000', '2929', '546', '592', '551']},
      'flows.salesStages.state.name':{$nin: ['noService', 'closedSale', 'houseAccount', 'consolidated']},
      'relationships.salesRep.targets.key': {$exists: 1}
  };

  db.app.opportunities.find(filter).readPref('secondary').hint('affinityPath').forEach(function(doc) {
    db.app.opportunities.update({_id: doc._id}, {$unset: {'relationships.salesRep': 1}, $push: {'tags': 'repRemAff'}});

    db.app.offers.update({
        "systemProperties.tenant" : "dell",
        "systemProperties.expiredOn" : ISODate("9999-01-01T00:00:00Z"),
        "relationships.opportunity.targets.key" : doc._id.valueOf()
      }, {$unset: {'relationships.salesRep': 1}, $push: {'tags': 'repRemAff'}}, false, true);
    
  });
});
