rs.slaveOk();
load('../common/helper.js');

var printObject = function(doc) {
   if (!doc) return;

   var product = getRel(doc, 'product');
   var customer = getRel(doc, 'customer');
   var asset = getRel(doc, 'covered');
   var quote = getRel(doc, 'quote');
   var coveredProduct = getRel(asset, 'covered');

   printVals([
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
   isoDate(extValue(asset, 'eosDate', false)),
   extValue(product, 'serviceLevel', false),
   extValue(coveredProduct, 'systemType', false),
   ]);

}


   printVals([
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


// 1. Find the tags
var oppfilter = {
  "systemProperties.tenant": "dell",
  "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
  "relationships.customer.targets.extensions.tenant.buId.value": buId,
};

if (qtr) oppfilter["extensions.master.clientBatchQuarter.value"] = qtr;

var exttags = db.app.opportunities.distinct('tags', oppfilter),
    tags= null;
if(exttags)  {
        tags = [];
        exttags.forEach(function(tg)  {
                tg && tags.push(tg);
        });
};
// 2. Find the offers and print them
var getClause = {
    "displayName": 1,
    "extensions": 1,
    "externalIds.id": 1,
    "systemProperties": 1,
    "amount.amount": 1,
    "targetAmount.amount": 1,
    "relationships.customer": 1,
    "relationships.covered": 1,
    "relationships.product": 1,
    "relationships.quote": 1,
    "resultReason": 1,
    "startDate": 1,
    "endDate": 1,
    };

var filter = {
  "systemProperties.tenant": "dell",
  "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
  "relationships.customer.targets.extensions.tenant.buId.value": buId,
};

//if (tags && tags.length > 0) filter.tags = {$in: tags};
if (qtr) filter["extensions.master.clientBatchQuarter.value"] = qtr;

var fetchOffers = function(tag)  {
        if(tag)  {
                filter['tags'] = tag;
        } else {
                delete filter['tags'];
        }
        db.app.offers.find(filter, getClause)
        .readPref('secondary')
        .addOption(DBQuery.Option.noTimeout)
        .forEach(printObject);
};
if(!tags || tags.length === 0) {
        fetchOffers();
} else {
        tags.forEach(fetchOffers);
}


