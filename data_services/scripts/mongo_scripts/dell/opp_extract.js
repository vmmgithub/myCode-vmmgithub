rs.slaveOk();

load('../common/helper.js'); 
        
print("Opp Id" + '\t' + "Opp Name" + '\t' + "SalesRep" + '\t' + "SalesRepId" 
    + '\t' + "CustomerId" + '\t' + "Customer" + '\t' + "Created On");
db.app.opportunities.find({
    "systemProperties.tenant" : "dell",
    "systemProperties.expiredOn" : ISODate("9999-01-01:00:00:00Z"),
    "tags": {$in: tags}
  }, {          
    }).addOption(DBQuery.Option.noTimeout).forEach(function(doc) {
        var displayName = doc.displayName;

        var salesRepRel= getRel(doc, 'salesRep');
        var salesRep = salesRepRel && salesRepRel.displayName;
        var salesRepUID = salesRepRel && (salesRepRel.id || salesRepRel.key);

        var customerRel = getRel(doc, 'customer');
        var customerId = customerRel && customerRel.key;
        var customer = customerRel && customerRel.displayName;

        print(doc._id.str + '\t' + displayName + '\t' + salesRep + '\t' + salesRepUID 
            + '\t' + customerId + '\t' + customer + '\t' + isoDate(doc.systemProperties.createdOn));
    }); 
