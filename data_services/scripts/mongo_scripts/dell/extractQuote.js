rs.slaveOk();
load('../common/helper.js');

printVals([
   'oppID',   'buId',   'baseQuoteID',  'baseQuoteAmt', 'baseQuoteCurr', 'baseQuoteNormAmt', 'baseQuoteNormCurr', 'primaryQuoteID', 'priQuoteAmt',
        'priQuoteCurr', 'priQuoteNormAmt', 'priQuoteNormCurr', 'latestQuoteID', 'ltQuoteAmt', 'ltQuoteCurr', 'ltQuoteNormAmt', 'ltQuoteNormCurr'
   ]);

var oppfilter = {
   "systemProperties.tenant": "dell",
     "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
       "relationships.customer.targets.extensions.tenant.buId.value": buId,
       };
if (qtr) oppfilter["extensions.master.clientBatchQuarter.value"] = qtr;

db.app.opportunities.find(oppfilter)
  .readPref('secondary')
  .addOption(DBQuery.Option.noTimeout)
  .forEach(function(d) {
  var OppID = d._id;
  var baseQuote = getRel(d, 'baseQuote');
  var latestQuote = getRel(d, 'latestQuote');
  var primaryQuote = getRel(d, 'primaryQuote');
  var baseQuoteKey = (baseQuote && getRelKeys(d, 'baseQuote'));
//  var baseQuoteKey = (baseQuote && baseQuote.key);
  var ltQuoteKey = (latestQuote && getRelKeys(d, 'latestQuote'));
  var priQuoteKey = (primaryQuote && getRelKeys(d, 'primaryQuote'));

   var pq = primaryQuote && priQuoteKey && db.app.quotes.find({_id: ObjectId(priQuoteKey)}, {amount: 1}).limit(1)[0];
   var priQuoteAmt = (pq && pq.amount && pq.amount.amount);
   var priQuoteCurr = (pq && pq.amount && pq.amount.code && pq.amount.code.displayName);
   var priQuoteNormAmt = (pq && pq.amount && pq.amount.normalizedAmount && pq.amount.normalizedAmount.amount);
   var priQuoteNormCurr = (pq && pq.amount && pq.amount.normalizedAmount && pq.amount.normalizedAmount.code && pq.amount.normalizedAmount.code.displayName);

   var bq = baseQuote && baseQuoteKey && db.app.quotes.find({_id: ObjectId(baseQuoteKey)}, {amount: 1}).limit(1)[0];
   var baseQuoteAmt = (bq && bq.amount && bq.amount.amount);
   var baseQuoteCurr = (bq && bq.amount && bq.amount.code && bq.amount.code.displayName);
   var baseQuoteNormAmt = (bq && bq.amount && bq.amount.normalizedAmount && bq.amount.normalizedAmount.amount);
   var baseQuoteNormCurr = (bq && bq.amount && bq.amount.normalizedAmount && bq.amount.normalizedAmount.code && bq.amount.normalizedAmount.code.displayName);

   var lq = latestQuote && ltQuoteKey && db.app.quotes.find({_id: ObjectId(ltQuoteKey)}, {amount: 1}).limit(1)[0];
   var ltQuoteAmt = (lq && lq.amount && lq.amount.amount);
   var ltQuoteCurr = (lq && lq.amount && lq.amount.code && lq.amount.code.displayName);
   var ltQuoteNormAmt = (lq && lq.amount && lq.amount.normalizedAmount && lq.amount.normalizedAmount.amount);
   var ltQuoteNormCurr = (lq && lq.amount && lq.amount.normalizedAmount && lq.amount.normalizedAmount.code && lq.amount.normalizedAmount.code.displayName);


printVals([OppID, buId, baseQuoteKey,baseQuoteAmt, baseQuoteCurr, baseQuoteNormAmt,baseQuoteNormCurr, priQuoteKey, priQuoteAmt, priQuoteCurr, priQuoteNormAmt,priQuoteNormCurr, ltQuoteKey, ltQuoteAmt, ltQuoteCurr, ltQuoteNormAmt, ltQuoteNormCurr ]);

});
