var tenant = "dell";

load('../common/helper.js');

////////////////////////////////////////////////////////////////////////
// Step 1. Find the opportunities
////////////////////////////////////////////////////////////////////////

// 1a: build a filter
var oppfilter = {
  "systemProperties.tenant": tenant,
  "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
};

if (startDate && startDate != 'undefined') {
  if (!oppfilter.resolutionDate) oppfilter.resolutionDate  = {};
  oppfilter.resolutionDate['$gt'] = ISODate(startDate);
};

if (endDate && endDate != 'undefined') {
  if (!oppfilter.resolutionDate) oppfilter.resolutionDate  = {};
  oppfilter.resolutionDate['$lt'] = ISODate(endDate);
};

//if (salesStages && salesStages != 'undefined') 
oppfilter['flows.salesStages.state.name'] = salesStages;

//oppfilter["relationships.customer.targets.extensions.tenant.buId.value"]= {$in: [ '2121', '584', '572', '552', '5151', '547', '5455', '1212', '2323', '1224', '3131', '3434', '1222', '5000', '2929', '546', '592', '551', ]}

// 1b: build a get clause
var oppGetClause = {
    'displayName': 1, 
    'relationships.customer': 1,
    'flows.salesStages.state': 1,
    'resolutionDate': 1,
    'relationships.primaryQuote.targets.key': 1,
    'relationships.quotes.targets.key': 1,
    'relationships.baseQuote.targets.key': 1,
};

var offGetClause = {
    "displayName": 1,
    "resultReason": 1,
    "startDate": 1, 
    "endDate": 1,
    "amount": 1,
    "targetAmount": 1,
    "extensions": 1,
    'result': 1,
    'extensions.master.batchType': 1,
    "relationships.predecessor.": 1,
    "relationships.product": 1,
    "relationships.covered": 1,
    "relationships.base": 1,
};

var assetGetClause = {
    "displayName": 1,
    "endDate": 1,
    "extensions.master.dateReceived": 1,
    "amount": 1,
    'systemProperties.dlOn': 1
};

////////////////////////////////////////////////////////////////////////
// 2. Print the headers for the file
////////////////////////////////////////////////////////////////////////

printVals([

  'OPP_ID', 
  'OPP_NAME', 
  'CUSTOMER', 
  'SALES_STAGE', 
  'RESOLUTION_DATE', 

  'OFFER_ID', 
  'OFFER_NAME',
  'RESULT_REASON', 
  'BATCH_TYPE',
  'OFFER_START_DATE', 
  'OFFER_END_DATE', 
  'OFFER_TARGET_AMT', 
  'OFFER_AMT', 

  'BUID', 
  'CLIENT_TERRITORY',
  'CUSTOMER_NUMBER',
  'COUNTRY',
  'SEGMENT',

  'ASSET_ID',
  'ASSET_NAME',
  'ASSET_EXP_DATE', 
  'ASST_RCV_DATE',
  'ASST_AMOUNT', 

  'COVERED_ASSET',
  'SERIAL_NUMBER',
  'SHIP_DATE',
  'EOS_DATE',

  'PRODUCT_NAME',
  'SKU', 
  'SERVICE_LEVEL', 
  'SERVICE_CLASS',

  'SYSTEM_TYPE',
  'LINE_OF_BUSINESS', 
  'BRAND_DESCRIPTION', 
]);

////////////////////////////////////////////////////////////////////////
// 3. Down to business
////////////////////////////////////////////////////////////////////////

db.app.opportunities.find(oppfilter, oppGetClause)
.readPref('secondary') .addOption(DBQuery.Option.noTimeout)
//.limit(5)
.forEach(function(opp) {

  // 3a. read the opp attributes
  var id = _id(opp);
  var customer = getRel(opp, 'customer');
  var salesStage = opp.flows && opp.flows.salesStages && opp.flows.salesStages.state && opp.flows.salesStages.state.displayName;
  var resolutionDate = isoDate(opp.resolutionDate);
  var quoteIds = [];
  if (getRelKey(opp, 'primaryQuote'))quoteIds.push(getRelKey(opp, 'primaryQuote'));
  if (getRelKey(opp, 'latestQuote'))quoteIds.push(getRelKey(opp, 'latestQuote'));
  if (getRelKey(opp, 'baseQuote'))quoteIds.push(getRelKey(opp, 'baseQuote'));

  var offFilter = {
    "systemProperties.tenant": tenant,
    "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
    "relationships.quote.targets.key": {$in: quoteIds},
  };

  // 3b. base this off offer attributes
  db.app.offers.find(offFilter, offGetClause).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(function(offer) {

      //print only closed offers
      if (!offer.result || offer.result.name != 'win') return;

       var assetId = getRelKey(offer, 'predecessor');
       var product = getRel(offer, 'product');
       var asset = getRel(offer, 'predecessor');
       var coveredAsset = getRel(offer, 'covered');
       var coveredProduct = getRel(coveredAsset, 'product');

       if (!product || !product.displayName) {
         var base = getRel(offer, 'base');
         product = base && base.product;
       }
  
      printVals([
        id,
        opp.displayName, 
        (customer && customer.displayName),
        salesStage,
        resolutionDate,

        _id(offer),
        offer.displayName,
        (offer.resultReason && (offer.resultReason.displayName || offer.resultReason.name)),
        extValue(offer, 'batchType', true),
        isoDate(offer.startDate),
        isoDate(offer.endDate),
        curr(offer.targetAmount, true),
        curr(offer.amount, true),

        extValue(customer, 'buId', false) ,
        extValue(customer, 'clientTerritory', false),
        extValue(customer, 'customerNumber', false),
        extValue(customer, 'country', false),
        extValue(customer, 'segment', false),

        assetId,
        (asset && asset.displayName),
        isoDate(asset && asset.endDate),
        isoDate(extValue(asset, 'dateReceived', true) || asset && asset.systemProperties && asset.systemProperties.dlOn),
        curr(asset && asset.amount, true),

        (coveredAsset && coveredAsset.displayName),
        extValue(coveredAsset, 'serialNumber', true),
        extValue(coveredAsset, 'shipDate', false),
        isoDate(extValue(coveredAsset, 'eosDate', false)),

        (product && product.displayName),
        extValue(product, 'sku', true),
        extValue(product, 'serviceLevel', false),
        extValue(product, 'serviceClass', false),

        extValue(coveredProduct, 'systemType', false),
        extValue(coveredProduct, 'lineOfBusiness', false),
        extValue(coveredProduct, 'brandDescription', false) ,
      ]);

  });

});

