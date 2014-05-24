rs.slaveOk();
load('../common/helper.js');

var printOpp = function(doc) {
   if (!doc) return;

   var displayName = doc.displayName;
   var uid = doc._id.valueOf();
   var customer = getRel(doc, 'customer');
   var buId = extValue(customer, 'buId', false);
//   var state = floValue(doc, 'state'); //ADDED IN HELPER.JS, need it ??
   var state =  doc.flows.salesStages['state'].displayName;
  // var quote = getRel(doc, 'quote');
   var primaryQuote = getRel(doc, 'primaryQuote');
   var targetAmount = (doc.targetAmount && doc.targetAmount.amount);
   var targetCurrency = (doc.targetAmount && doc.targetAmount.code && doc.targetAmount.code.displayName);
   var amount = (doc.amount && doc.amount.amount);
   var currency = (doc.amount && doc.amount.code && doc.amount.code.displayName);
   var normalizedAmount = (doc.amount && doc.amount.normalizedAmount && doc.amount.normalizedAmount.amount);
   var normalizedCurrency = (doc.amount && doc.amount.normalizedAmount.code && doc.amount.normalizedAmount.code.displayName);

   var q = primaryQuote && db.app.quotes.find({_id: ObjectId(primaryQuote.key)}, {amount: 1}).limit(1)[0];
   var priQuoteAmt = (q && q.amount && q.amount.amount);
   var priQuoteCurr = (q && q.amount && q.amount.code && q.amount.code.displayName);
   var priQuoteNormAmt = (q && q.amount && q.amount.normalizedAmount && q.amount.normalizedAmount.amount);
   var priQuoteNormCurr = (q && q.amount && q.amount.normalizedAmount && q.amount.normalizedAmount.code && q.amount.normalizedAmount.code.displayName);
   var splitTo = getRel(doc, 'splitTo');



   printVals([
        displayName,
        uid,
        (customer && customer.displayName),
        (customer && customer.key),
        extValue(doc, 'businessLine', false),
        extValue(doc, 'clientBatchQuarter', true),
        targetAmount,
        targetCurrency,
        amount,
        currency,
        normalizedAmount,
        normalizedCurrency,
        buId,
        extValue(doc, 'country', true),
        state,
        extValue(customer, 'customerNumber', false),
        (primaryQuote && primaryQuote.displayName),
        (primaryQuote && primaryQuote.key),
        priQuoteAmt,
        priQuoteCurr,
        priQuoteNormAmt,
        priQuoteNormCurr,
        splitTo,
        (splitTo && splitTo.displayName),
   ]);
}

printVals([
        'displayName' ,
        'uid' ,
        'customerName' ,
        'customerId' ,
        'businessLine',
        'clientBatchQuarter',
        'targetAmount',
        'targetCurrency',
        'amount',
        'currency',
        'normalizedAmount',
        'normalizedCurrency',
        'buId',
        'country',
        'salesState',
        'customerNumber',
        'primaryQuoteName',
        'primaryQuoteId',
        'primaryQuoteAmt',
        'primaryQuoteCurrency',
        'primaryQuotNormAmt',
        'primaryQuotNormCurrency',
        'splitTo',
        'splitToOPPName',
]);

var getClause = {
    "displayName": 1,
    "extensions": 1,
    'externalIds.id': 1,
    "systemProperties": 1,
    "amount.amount": 1,
    "amount.code": 1,
    "amount.normalizedAmount.amount": 1,
    "amount.normalizedAmount.code": 1,
    "targetAmount.amount": 1,
    "targetAmount.code": 1,
    "relationships.customer": 1,
    "relationships.primaryQuote": 1,
    "relationships.quote": 1,
    "relationships.splitTo": 1,
    "flows": 1,
    };

var filter = {
"systemProperties.tenant": "dell",
"systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
"relationships.customer.targets.extensions.tenant.buId.value": buId,
"extensions.master.clientBatchQuarter.value": qtr,
// "flows.salesStages.state.name": {$in:['quoteCompleted','quoteDelivered','customerCommitment','poReceived']},
// "extensions.master.clientTheatre.value.name": "eumea"
};

db.app.opportunities.find(filter, getClause)
.readPref('secondary')
.addOption(DBQuery.Option.noTimeout)
.forEach(printOpp);
        
