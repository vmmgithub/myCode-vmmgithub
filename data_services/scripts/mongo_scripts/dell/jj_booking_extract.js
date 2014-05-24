rs.slaveOk();
load('../common/helper.js');

var filter = {
"systemProperties.tenant": "dell",
"systemProperties.expiredOn": ISODate("9999-01-01")
};

print("Name" + "\t" + "_id" + "\t" + "Status" + "\t" + "Amount" + "\t" + "Currency" + "\t" + "PaymentDate" + "\t" + "PaymentID" + "\t" + "SalesOrderID" + "\t" + "LineItemsAmount" + "\t" + "LineItemsCurrency" + "\t" + "Customer");

db.app.bookings.find(filter)
//.limit(10)
.readPref('secondary')
.addOption(DBQuery.Option.noTimeout)
.forEach(function(d) {
var name = d.displayName;
var status = d.flows && d.flows.bookingStages && d.flows.bookingStages.state && d.flows.bookingStages.state.displayName;
var customer = getRel(d, 'customer');

print(name + "\t" + d._id.valueOf() + "\t" + status + "\t" + (d.amount && d.amount.amount) + "\t" + (d.amount && d.amount.code && d.amount.code.displayName) + "\t" + isoDate(d.poDate) + "\t" + d.poNumber + "\t" + d.soNumber+ "\t" + (d.amount && d.amount.amount) + "\t" +  (d.amount && d.amount.code.displayName) + "\t" + (customer && customer.displayName));

});
