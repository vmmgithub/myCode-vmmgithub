rs.slaveOk();

load('../common/helper.js');

print("Service Asset Name" + '\t' + "Service Asset UID" + '\t' + "Primary" + '\t' + "Asset Tag" + '\t' + "Contract Number" 
		+ '\t' + "End Date" + '\t' + "Batch Quarter" + '\t' + "Ship Date" + '\t' + "Segment Code" + '\t' + "Service Class" 
		+ '\t' + "Covered EOS" + '\t' + "Product EOS" + '\t' + 'Customer Id' + '\t' + 'Covered Id' + '\t' + "Has Opp" 
    + '\t' + "Customer UID"  + '\t' + "Covered UID"+ '\t' + 'Product Id' + '\t' + "Product UID" 
    + '\t' + 'Affinity Id' + '\t' + "Affinity Path" );
db.app.assets.find({
    "systemProperties.tenant" : "dell",
    "systemProperties.expiredOn" : ISODate("9999-01-01:00:00:00Z"),
    "type" :"app.asset/service",
    "relationships.customer.targets.extensions.tenant.buId.value" : buid,
    "endDate" : { $gte: ISODate(startDate), $lte: ISODate(endDate) }
  }, {
    "displayName": 1,
    "startDate" : 1,
    "endDate" : 1,
    "extensions": 1,
    "associatedOpportunity": 1,
    "relationships.predecessor.targets": 1,
    "relationships.covered.targets" : 1,
    "relationships.product.targets" : 1,
    "relationships.customer.targets" : 1,
    'externalIds.id': 1,
    "systemProperties": 1
    }).addOption(DBQuery.Option.noTimeout).forEach(function(doc) {
        var displayName = doc.displayName;
        var uid = id(doc);
        var startDate = isoDate(doc.startDate);
        var endDate = isoDate(doc.endDate);
        var primary = extValue(doc, 'primary', false);
        var localChannelCode = extValue(doc, 'localChannelCode', false);
        var contractNumber = extValue(doc, 'contractNumber', true);
        var serialNumber = extValue(doc, 'serialNumber', true);
        var batchQuarter = extValue(doc, 'batchQuarter', true);
        var shipDate = isoDate(extValue(doc, 'shipDate', false));
        var serviceClass = extValue(doc, 'serviceClass', false);

        var customerRel = getRel(doc, 'customer');
        var segment = customerRel && extValue(customerRel, 'segment', false);
        var coveredRel= getRel(doc, 'covered');;
        var customerUID = customerRel && customerRel.id;
        var customerId = customerRel && customerRel.key;
        var coveredId = coveredRel && coveredRel.key;
        var coveredUID = coveredRel && coveredRel.id;
        var productRel = getRel(doc, 'product');
        var productUID = productRel && productRel.id;
        var productId = productRel && productRel.key;
        var aeosDate = coveredRel && extValue(coveredRel, 'eosDate', false);
        aeosDate = isoDate(aeosDate);

        var affinityOrganization = customerRel && getRel(customerRel, 'affinityOrganization');
        var affinityId = affinityOrganization && extValue(affinityOrganization, 'affinityId', false);
        var affinityPath = affinityOrganization && extValue(affinityOrganization, 'affinityPath', false);

        var coveredProduct = coveredRel && getRel(coveredRel, 'product');
        var peosDate = coveredProduct && extValue(coveredProduct, 'eosDate', false);
        peosDate = isoDate(peosDate);

        print(displayName + '\t' + uid + '\t' + primary + '\t' + serialNumber + '\t' + contractNumber + '\t' + endDate 
      	   + '\t' + batchQuarter + '\t' + shipDate + '\t' + segment + '\t' + serviceClass + '\t' 
      	   + aeosDate + '\t' + peosDate + '\t' + customerId + '\t' + coveredId + '\t' + doc.associatedOpportunity 
           + '\t' + customerUID + '\t' + coveredUID + '\t' + productId + '\t' + productUID
           + affinityId + '\t' + affinityPath);
    });

