rs.slaveOk();
load('../common/helper.js');

var printOpp = function(doc) {
   if (!doc) return;

   var displayName = doc.displayName;
   var uid = doc._id.valueOf();
   var customer = getRel(doc, 'customer');
   var buId = extValue(customer, 'buId', false);
   var quote = getRel(doc, 'quote');
   var baseQuote = getRel(doc, 'baseQuote');
   var primaryQuote = getRel(doc, 'primaryQuote');
   var latestQuote = getRel(doc, 'latestQuote');
   var salesRep = getRel(doc, 'salesRep');
   var salesRep2 = getRel(doc, 'salesRep', 1);
   var salesRep3 = getRel(doc, 'salesRep', 2);
   var assignedTeam = getRel(doc, 'assignedTeam');
   var targetAmount = doc.targetAmount && doc.targetAmount.amount;

   printVals([
	displayName, 
	uid, 
	(customer && customer.displayName), 
	(customer && customer.key), 
	(salesRep && salesRep.displayName),
	(salesRep && salesRep.key),
        (salesRep2 && salesRep2.displayName),
        (salesRep2 && salesRep2.key),
        (salesRep3 && salesRep3.displayName),
        (salesRep3 && salesRep3.key),
	extValue(doc, 'businessLine', true),
	extValue(doc, 'clientBatchQuarter', true),
	(primaryQuote && primaryQuote.displayName), 
	(primaryQuote && primaryQuote.key), 
	(baseQuote && baseQuote.displayName), 
	(baseQuote && baseQuote.key), 
	targetAmount, 
	buId, 
	extValue(customer, 'customerNumber', false), 
	(latestQuote && latestQuote.displayName), 
	(latestQuote && latestQuote.key), 
	(quote && quote.displayName), 
	(quote && quote.key), 
	(assignedTeam && assignedTeam.displayName), 
        extValue(doc, 'clientTerritory', true),
        extValue(doc, 'country', true),

   ]);
}

printVals([
	'displayName' , 
	'uid' , 
	'customerName' , 
	'customerId' , 
	'salesRepName',
	'salesRepId',
        'salesRepName2',
        'salesRepId2',
        'salesRepName3',
        'salesRepId3',
	'businessLine',
	'clientBatchQuarter',
	'primaryQuoteName', 
	'primaryQuoteId', 
	'baseQuoteName', 
	'baseQuoteId', 
	'targetAmount',
	'buId',
	'customerNumber',
	'latestQuoteName', 
	'latestQuoteId', 
	'quoteName', 
	'quoteId',
	'team', 
        'clientTerritory',
        'country',
	
]);

var getClause = {
    "displayName": 1,
    "extensions": 1,
    'externalIds.id': 1,
    "systemProperties": 1,
    "amount.amount": 1,
    "targetAmount.amount": 1,
    "relationships.customer": 1,
    "relationships.baseQuote": 1,
    "relationships.primaryQuote": 1,
    "relationships.latestQuote": 1,
    "relationships.quote": 1,
    "relationships.salesRep": 1,
    "relationships.assignedTeam": 1,
    };

var filter = {
"systemProperties.tenant": "dell",
"systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
"relationships.customer.targets.extensions.tenant.buId.value": buId,
"extensions.master.clientBatchQuarter.value": qtr
};

db.app.opportunities.find(filter, getClause)
.readPref('secondary')
.addOption(DBQuery.Option.noTimeout)
.forEach(printOpp);
