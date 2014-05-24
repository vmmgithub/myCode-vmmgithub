load('../common/helper.js');

////////////////////////////////////////////////////////////////////////
//////// Step 0. Print Header
//////////////////////////////////////////////////////////////////////////////

printVals([
  'BookingName', 'BookingID', 'BookingStatus', 'LineItemsAmount', 'LineItemsCurrency', 'Customer', 'BUID', 'Country',
  'PaymentID', 'PaymentAmount', 'PaymentDate', 'SalesOrderID', 'SalesOrderAmount', 'SalesOrderDate',
  'LineItemName', 'LineItemID', 'DiscountAmount', 'DiscountPercentage', 'Amount', 'NormAmount', 'StartDate', 'EndDate',
  'Predecessor', 'Successor', 'Product', 'ProductCategory', 'ProductServiceClass', 'SKU', ]);

////////////////////////////////////////////////////////////////////////
////// Step 1. Filter for Booking Line Item
////////////////////////////////////////////////////////////////////////////

var bookLineFilter = {
  "systemProperties.tenant": "dell",
  "systemProperties.expiredOn": ISODate("9999-01-01"),
  "type": "app.lineitem/booking",
};

var productGetClause = {
  "displayName": 1,
  "extensions.master.category": 1,
  "extensions.tenant.serviceClass": 1,
  "extensions.master.sku": 1,
};


db.app.lineitems.find(bookLineFilter)
  .readPref('secondary')
  .addOption(DBQuery.Option.noTimeout)
  .forEach(function(d) {
  var bookLineName = d.displayName;
  var bookLineID = d._id;
  var endDate = d.endDate;
  var startDate = d.startDate;
  var amount = d.amount && d.amount.amount;
  var normAmount = d.amount && d.amount.normalizedAmount && d.amount.normalizedAmount.amount;
  var discount = d.discountAmount && d.discountAmount.amount;

  var pred = getRel(d, 'predecessor');
  var succ = getRel(d, 'successor');
  var product = getRel(d, 'product');
  var bookingKey = d.headerDocument.headerKey;

  /////// Getting the Product Record to obtain it's extensions as there are no PROJECTIONS for Product extensions
  var prdKey = getRelKey(d, 'product');
  var prd;

  if (prdKey) {
    prd = db.app.products.find({
      _id: ObjectId(prdKey)
    }, productGetClause).limit(1);
  }
  prd = prd && prd[0];
  var category = extValue(prd, 'category', true);
  var serviceClass = extValue(prd, 'serviceClass', false);
  var sku = extValue(prd, 'sku', true);
  
  
  ////////////////////////////////////////////////////////////////////////
  ////// Step 2. Filter for Bookings
  ////////////////////////////////////////////////////////////////////////////

  var bookingFilter = {
    "systemProperties.tenant": "dell",
    "systemProperties.expiredOn": ISODate("9999-01-01"),
    "type": "app.booking/sales",
    "_id": ObjectId(bookingKey)
  };

  if (strtDate && strtDate != 'undefined') {
    if (!bookingFilter.soDate) bookingFilter.soDate = {};
    bookingFilter.soDate['$gt'] = ISODate(strtDate);
  }

  if (finalDate && finalDate != 'undefined') {
    if (!bookingFilter.soDate) bookingFilter.soDate = {};
    bookingFilter.soDate['$lt'] = ISODate(finalDate);
  }
  if (bookStages && bookStages != 'undefined') bookingFilter['flows.bookingStages.state.name'] = bookStages;

  db.app.bookings.find(bookingFilter)
    .readPref('secondary')
    .addOption(DBQuery.Option.noTimeout)
    .forEach(function(b) {
    var bookingName = b.displayName;
    var bookingStatus = b.flows && b.flows.bookingStages && b.flows.bookingStages.state && b.flows.bookingStages.state.displayName;
    var customer = getRel(b, 'customer');
    var buId = extValue(customer, 'buId', false);
    var country = extValue(customer, 'country', false);
    var poAmount = b.poAmount && b.poAmount.amount;
    var poDate = b.poDate;
    var poNumber = b.poNumber;
    var soAmount = b.soAmount && b.soAmount.amount;
    var soNumber = b.soNumber;
    var soDate = b.soDate;

    ////////////////////////////////////////////////////////////////////////
    ////// 3. Print the values
    ////////////////////////////////////////////////////////////////////////////

    printVals([bookingName, bookingKey, bookingStatus, (b.amount && b.amount.amount), (b.amount && b.amount.code && b.amount.code.displayName), (customer && customer.displayName), buId, country,
    poNumber, poAmount, isoDate(poDate), soNumber, soAmount, isoDate(soDate),
    bookLineName, bookLineID, discount, d.discountPercentage, amount, normAmount, isoDate(startDate), isoDate(endDate),
    (pred && pred.displayName), (succ && succ.displayName), (product && product.displayName), category, serviceClass, sku, ]);


  });
});
