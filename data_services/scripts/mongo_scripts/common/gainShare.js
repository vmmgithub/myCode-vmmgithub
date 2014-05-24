rs.slaveOk();
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

if (salesStages && salesStages != 'undefined') 
  oppfilter['flows.salesStages.state.name'] = salesStages;

// 1b: build a get clause
var oppGetClause = {
    'displayName': 1, 
    'relationships.customer.targets.displayName': 1,
    'flows.salesStages.state': 1,
    'resolutionDate': 1,
    'relationships.primaryQuote.targets.key': 1,
    'relationships.quotes.targets.key': 1,
    'relationships.baseQuote.targets.key': 1,
    'extensions.master.clientTerritory': 1,
};

var offGetClause = {
    "displayName": 1,
    "resultReason": 1,
    "startDate": 1, 
    "endDate": 1,
    "amount": 1,
    "targetAmount": 1,
    'extensions.master.batchType': 1,
    "relationships.predecessor.targets.key": 1,
    "relationships.product.targets.displayName": 1,
};

var assetGetClause = {
    "displayName": 1,
    "startDate": 1,
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
  'TERRITORY',

  'OFFER_ID', 
  'OFFER_NAME',
  'RESULT_REASON', 
  'BATCH_TYPE',
  'OFFER_START_DATE', 
  'OFFER_END_DATE', 
  'OFFER_TARGET_AMT', 
  'OFFER_AMT', 

  'ASSET_ID',
  'ASSET_NAME',
  'ASSET_START_DATE', 
  'ASSET_EXP_DATE', 
  'ASST_RCV_DATE',
  'ASST_AMOUNT', 
 
  'PRODUCT_NAME',
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
  var customer = getRel(opp, 'customer') && getRel(opp, 'customer').displayName;
  var salesStage = opp.flows && opp.flows.salesStages && opp.flows.salesStages.state && opp.flows.salesStages.state.displayName;
  var resolutionDate = isoDate(opp.resolutionDate);
  var quoteId = getRelKey(opp, 'primaryQuote') || getRelKey(opp, 'quote') || getRelKey(opp, 'baseQuote') || getRelKey(opp, 'base');

  var offFilter = {
    "systemProperties.tenant": tenant,
    "systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
    "relationships.quote.targets.key": quoteId,
  };

  // 3b. base this off offer attributes
  db.app.offers.find(offFilter, offGetClause).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(function(offer) {

      var assetId = getRelKey(offer, 'predecessor');
      var product = getRel(offer, 'product');

      //3c. read the previous asset
      var asset = db.app.assets.find({_id: ObjectId(assetId)}, assetGetClause).limit(1);
      asset = asset && asset[0];

      printVals([
        id,
        opp.displayName, 
        customer,
        salesStage,
        resolutionDate,
        extValue(opp, 'clientTerritory', true),

        _id(offer),
        offer.displayName,
        (offer.resultReason && (offer.resultReason.displayName || offer.resultReason.name)),
        extValue(offer, 'batchType', true),
        isoDate(offer.startDate),
        isoDate(offer.endDate),
        curr(offer.targetAmount, true),
        curr(offer.amount, true),

        assetId,
        (asset && asset.displayName),
        isoDate(asset.startDate),
        isoDate(asset.endDate),
        isoDate(extValue(asset, 'dateReceived', true) || asset.systemProperties.dlOn),
        curr(asset.amount, true),

        (product && product.displayName),
      ]);

  });

});

