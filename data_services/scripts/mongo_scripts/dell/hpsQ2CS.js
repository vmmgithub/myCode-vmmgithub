rs.slaveOk();
load('../common/helper.js');

var printObject = function(doc) {
   if (!doc) return;

   var product = getRel(doc, 'product');
   var customer = getRel(doc, 'customer');
   var asset = getRel(doc, 'covered');
   var quote = getRel(doc, 'quote');
   var coveredProduct = getRel(asset, 'covered');

   if (mode == 2) printVals([
   doc.displayName,
   doc._id.valueOf(),
   extValue(customer, 'buId', false) ,
   extValue(product, 'lineOfBusiness', false),
   extValue(product, 'brandDescription', false) ,
   extValue(customer, 'clientTerritory', false),
   extValue(product, 'serviceClass', false),
   (product && product.displayName) ,
   extValue(product, 'sku', true),
   (asset && asset.displayName),
   extValue(asset, 'serialNumber', true),
   (doc.targetAmount && doc.targetAmount.amount),
   (doc.amount && doc.amount.amount) ,
   (quote && quote.key),
   (doc.resultReason && doc.resultReason.displayName),
   isoDate(doc.startDate),
   isoDate(doc.endDate),
   extValue(customer, 'customerNumber', false),
   extValue(customer, 'country', false),
   extValue(customer, 'segment', false),   
   extValue(asset, 'shipDate', false),   
   extValue(asset, 'eosDate', false), 
   extValue(product, 'serviceLevel', false), 
   extValue(coveredProduct, 'systemType', false), 
   ]);

}

var getRelKey = function (doc, name) {
  return (doc && doc.relationships && doc.relationships[name] && doc.relationships[name].targets && doc.relationships[name].targets[0] && doc.relationships[name].targets[0].key);
};

// 1. Find the tags
var oppfilter = {
  "systemProperties.tenant": "dell",
  "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
  "relationships.customer.targets.extensions.tenant.buId.value": {$in: [ '2121', '584', '572', '552', '5151', '547', '5455', '1212', '2323', '1224', '3131', '3434', '1222', '5000', '2929', '546', '592', '551', ]}, };

//oppfilter["extensions.master.clientBatchQuarter.value"] = {$in: ['FY14Q2', 'FY14Q3', 'FY14Q1', 'FY14Q4']};
oppfilter.resolutionDate = {$gt: ISODate('2012-11-30'), $lt: ISODate('2013-09-01')}
oppfilter['flows.salesStages.state.name'] = 'closedSale';

var quotes = {};
if (mode == 1) printVals(['UID', 'OPPNAME', 'BASEQUOTE', 'PRIMARYQUOTE', 'LATESTQUOTE', 'BASE', 'QUOTE', 'RESOLUTIONDATE']);
db.app.opportunities.find(oppfilter).readPref('secondary') .addOption(DBQuery.Option.noTimeout) .forEach(function(opp) {
//quotes[getRelKey(opp, 'baseQuote')] = 1;
quotes[getRelKey(opp, 'primaryQuote')] = 1;
//quotes[getRelKey(opp, 'latestQuote')] = 1;
//quotes[getRelKey(opp, 'base')] = 1;
//quotes[getRelKey(opp, 'quote')] = 1;
if (mode == 1) printVals([opp._id.valueOf(), opp.displayName, getRelKey(opp, 'baseQuote'), getRelKey(opp, 'primaryQuote'), getRelKey(opp, 'latestQuote'), getRelKey(opp, 'base'), getRelKey(opp, 'quote'), isoDate(opp.resolutionDate)]);
});

var quoteIds = [];
for (var quoteId in quotes) {
	quoteIds.push(quoteId);
}

// 2. Find the offers and print them
var getClause = {
    "displayName": 1,
    "extensions": 1,
    'externalIds.id': 1,
    "systemProperties": 1,
    "amount.amount": 1,
    "targetAmount.amount": 1,
    "relationships.customer": 1,
    "relationships.covered": 1,
    "relationships.product": 1,
    "relationships.quote": 1,
    "resultReason": 1,
    "startDate": 1, "endDate": 1
    };

var filter = {
  "systemProperties.tenant": "dell",
  "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
  "relationships.quote.targets.key": {$in: quoteIds},
};

if (mode == 2) printVals([
   'displayName',
   'uid',
   'buId',
   'lineOfBusiness',
   'brandDescription',
   'clientTerritory',
   'serviceClass',
   'productName',
   'sku',
   'assetName',
   'serialNumber',
   'targetAmount',
   'amount',
   'quote' ,
   'resultReason',
   'startDate',
   'endDate',
   'customerNumber',
   'country',
   'segment',
   'shipDate',
   'eosDate',
   'serviceLevel',
   'systemType',
   ]);

db.app.offers.find(filter, getClause)
.readPref('secondary')
.addOption(DBQuery.Option.noTimeout)
.forEach(printObject);

