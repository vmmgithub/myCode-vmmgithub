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
   var assignedTeam2 = getRel(doc, 'assignedTeam', 1);
   var assignedTeam3 = getRel(doc, 'assignedTeam', 2);
   var targetAmount = doc.targetAmount && doc.targetAmount.amount;
   var commitLevel = doc.commitLevel && doc.commitLevel.displayName;
   var engageType = extValue(doc, 'directChannel', true);
   var country = extValue(doc, 'country', true);
   var territory = extValue(doc, 'clientTerritory', true);
   var region = extValue(doc, 'clientRegion', true);
   var segment = extValue(customer, 'segment', false);
   var state =  doc.flows.salesStages['state'].displayName;


   printVals([
        displayName,
        uid,
        state,
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
        (assignedTeam && assignedTeam.key),
        (assignedTeam2 && assignedTeam2.displayName),
        (assignedTeam2 && assignedTeam2.key),
        (assignedTeam3 && assignedTeam3.displayName),
        (assignedTeam3 && assignedTeam3.key),
        commitLevel,
        engageType,
        country,
        territory,
        region,
        segment,

   ]);
}

printVals([
        'displayName' ,
        'uid' ,
        'salesStage',
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
        'AssignedTeamId',
        'AssignedTeam',
        'AssignedTeamId2',
        'AssignedTeam2',
        'AssignedTeamId3',
        'AssignedTeam3',
        'commitLevel',
        'engagementType',
        'country',
        'clientTerritory',
        'clientRegion',
        'segment',

]);

var getClause = {
    "displayName": 1,
    "extensions": 1,
    'externalIds.id': 1,
    "systemProperties": 1,
    "amount.amount": 1,
    "targetAmount.amount": 1,
    "targetAmount.targetDate": 1,
    "commitLevel": 1,
    "relationships.customer": 1,
    "relationships.baseQuote": 1,
    "relationships.primaryQuote": 1,
    "relationships.latestQuote": 1,
    "relationships.quote": 1,
    "relationships.salesRep": 1,
    "relationships.assignedTeam": 1,
    "flows": 1,
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

