rs.slaveOk();
load('../common/helper.js');

var filter = {
"systemProperties.tenant": "dell",
"systemProperties.expiredOn": ISODate("9999-01-01"),
"type": "app.lineitem/booking"
};

print("Name" + "\t" + "_id" + "\t" + "DiscountAmount" + "\t" + "DiscountPercentage" + "\t" + "Amount" + "\t" + "EndDate" + "\t" + "StartDate" + "\t" + "Predecessor" + "\t" + "Successor" + "\t" + "Product" + "\t" + "ProductCategory" + "\t" + "ProductServiceClass" + "\t" + "SKU");

db.app.lineitems.find(filter)
//.limit(10)
.readPref('secondary')
.addOption(DBQuery.Option.noTimeout)
.forEach(function(d) {
var name = d.displayName;
var endDate = d.endDate;
var startDate = d.startDate;
var amount = d.amount && d.amount.amount;
var discount = d.discountAmount && d.discountAmount.amount;

var pred = getRel(d, 'predecessor');
var succ = getRel(d, 'successor');
var product = getRel(d, 'product');
var category = extValue(product, 'category', true);
var serviceClass = extValue(product, 'serviceClass', false);
var sku = extValue(product, 'sku', true);

print(name + "\t" + d._id.valueOf() + "\t" + discount + "\t" + d.discountPercentage + "\t" + amount + "\t" + isoDate(endDate) + "\t" + isoDate(startDate) + "\t" + (pred && pred.displayName) + "\t" + (succ && succ.displayName) + "\t" + (product && product.displayName) + "\t" + category + "\t" + serviceClass + "\t" + sku + "\t" + d.headerDocument.headerKey);

});

