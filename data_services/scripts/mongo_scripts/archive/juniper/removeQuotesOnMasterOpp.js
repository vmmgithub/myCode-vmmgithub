load('../common/helper.js');

var f = {'systemProperties.tenant': 'juniper', 'systemProperties.expiredOn': ISODate('9999-01-01'), 'relationships.subordinateOpportunity.targets.key': {$exists: 1}, 'relationships.quote.targets.key': {$exists: 1}};

db.app.opportunities.find(f).forEach(function(opp) {
   printVals([_id(opp), getRelKeys(opp, 'quote'), 'quote']);
   db.app.opportunities.update(f, {$unset: {'relationships.quote': 1}});
});

var f = {'systemProperties.tenant': 'juniper', 'systemProperties.expiredOn': ISODate('9999-01-01'), 'relationships.subordinateOpportunity.targets.key': {$exists: 1}, 'relationships.primaryQuote.targets.key': {$exists: 1}};

db.app.opportunities.find(f).forEach(function(opp) {
   printVals([_id(opp), getRelKeys(opp, 'primaryQuote'), 'primaryQuote']);
   db.app.opportunities.update(f, {$unset: {'relationships.primaryQuote': 1}});
});

