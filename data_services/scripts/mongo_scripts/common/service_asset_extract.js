load('./helper.js');

printVals([
    'Service Asset ID', 
    'Service Asset UID', 
    'Service Asset Name', 
    'Start Date', 
    'End Date', 
    'Batch Quarter', 
    'Customer Id', 
    'Covered Id', 
    'Has Opp', 
    'Customer UID' , 
    'Covered UID' , 
    'Product Id', 
    'Product UID',
]);


db.app.assets.find({
    'systemProperties.tenant' : tenant,
    'systemProperties.expiredOn' : ISODate('9999-01-01:00:00:00Z'),
    'type' :'app.asset/service',
    //'endDate' : { $gte: ISODate(startDate), $lt: ISODate(endDate) }
  }, {
    'displayName': 1,
    'startDate' : 1,
    'endDate' : 1,
    'extensions.master.batchQuarter': 1,
    'associatedOpportunity': 1,
    'relationships.customer.targets' : 1,
    'relationships.covered.targets' : 1,
    'relationships.product.targets' : 1,
    'externalIds.id': 1,
    'systemProperties': 1
    }).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(function(doc) {

        var customerRel = getRel(doc, 'customer');
        var coveredRel= getRel(doc, 'covered');;
        var customerUID = customerRel && customerRel.id;
        var customerId = customerRel && customerRel.key;
        var coveredId = coveredRel && coveredRel.key;
        var coveredUID = coveredRel && coveredRel.id;
        var productRel = getRel(doc, 'product');
        var productUID = productRel && productRel.id;
        var productId = productRel && productRel.key;

        printVals([
            _id(doc),
            id(doc), 
            doc.displayName, 
            isoDate(doc.startDate),
            isoDate(doc.endDate),
            extValue(doc, 'batchQuarter', true),
            doc.associatedOpportunity, 
            customerId, 
            customerUID, 
            coveredId, 
            coveredUID, 
            productId, 
            productUID,
        ]);

    });

db.getLastError();
