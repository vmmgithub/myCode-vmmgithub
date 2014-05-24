rs.slaveOk();

load('../common/helper.js');

print("UID" + '\t' + "Offer Name" + '\t' + "Start Date" + '\t' + "End Date" + '\t' + "Covered" 
    + '\t' + "SalesRep" + '\t' + "SalesRepId" + '\t' + "Covered UId" 
    + '\t' + "CustomerId" + '\t' + "OpportunityId" + '\t' + "Primary"  
    + '\t' + "Opportunity"+ '\t' + "Customer"+ '\t' + "MongoID");
db.app.offers.find({
    "systemProperties.tenant" : "dell",
    "systemProperties.expiredOn" : ISODate("9999-01-01:00:00:00Z"),
    tags: {$in: tags}
    //"tags": {$in: ['iLPmfTdq', 'FqgBMCVZ']} //Fracee
    //"tags": {$in: ['bzYoVmbs', 'aNlUbRMD', 'tvQSUlMY', 'RqVPsrxS', 'qUzYhKuO']}//UK
  }, {
    }).addOption(DBQuery.Option.noTimeout).forEach(function(doc) {
        var displayName = doc.displayName;
        var startDate = isoDate(doc.startDate);
        var endDate = isoDate(doc.endDate);
        var pcn = extValue(doc, 'existingContractNumber', true);
        var primary = extValue(doc, 'primary', false);
        var predRel = getRel(doc, 'predecessor');
        var tag = predRel && extValue(predRel, 'serialNumber', true);

        var coveredRel= getRel(doc, 'covered');
        var coveredId = coveredRel && coveredRel.key;
        var coveredUID = coveredRel && coveredRel.id;

        var salesRepRel= getRel(doc, 'salesRep');
        var salesRep = salesRepRel && salesRepRel.displayName;
        var salesRepUID = salesRepRel && (salesRepRel.id || salesRepRel.key);

        var customerRel = getRel(doc, 'customer');
        var customerId = customerRel && customerRel.key;
        var customer = customerRel && customerRel.displayName;

        var opportunityRel = getRel(doc, 'opportunity');
        var opportunity = opportunityRel && opportunityRel.displayName;
        var opportunityId = opportunityRel && opportunityRel.key;

        print(tag+'-'+pcn + '\t' + displayName + '\t' + startDate + '\t' + endDate + '\t' + coveredId 
                + '\t' + salesRep + '\t' + salesRepUID + '\t' + coveredUID 
                + '\t' + customerId + '\t' + opportunityId + '\t' + primary
                + '\t' + opportunity + '\t' + customer + '\t' + doc._id.valueOf());
    });

