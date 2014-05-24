rs.slaveOk();

var id = function (doc) {
    return doc.externalIds && doc.externalIds[0] && doc.externalIds[0].id;
}

var isoDate = function (dt) {
    if (dt)
       return dt.getFullYear() + '-' + (dt.getMonth()+1) + '-' + dt.getDate();
    else 
        return '';
}

var extValue = function (doc, name, isMaster) {
  var e = isMaster ? 'master': 'tenant'; 
  if (doc.extensions && doc.extensions[e] && doc.extensions[e][name] && doc.extensions[e][name].value) {
    return (doc.extensions[e][name].value.displayName || doc.extensions[e][name].value);
  } else {
    return '';
  }
}

var relName = function (doc, name) {
  return doc.relationships && doc.relationships[name]
            && doc.relationships[name].targets && doc.relationships[name].targets[0]
            && (doc.relationships[name].targets[0].displayName || doc.relationships[name].targets[0].id);
}

print("Service Asset Name" + '\t' + "Service Asset UID" + '\t' + "Primary" + '\t' + "Asset Tag" + '\t' + "Contract Number" + '\t' + "End Date" + 
		'\t' + "Batch Quarter" + '\t' + "Ship Date" + '\t' + "Segment Code" + '\t' + "Service Class" + '\t' + 
		"Covered EOS" + '\t' + "Product EOS" + '\t' + 'Customer Id' + '\t' + 'Covered Id' + '\t' + 'Has Opp');
db.app.assets.find({
    "systemProperties.tenant" : "dell",
    "systemProperties.expiredOn" : ISODate("9999-01-01:00:00:00Z"),
    "type" :"app.asset/service",
    "extensions.tenant.buId.value" : "202",
    "endDate" : { $gte: ISODate("2013-05-04T00:00:00Z"), $lt: ISODate("2013-08-03T00:00:00Z") }
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
        var customerRel = doc.relationships.customer && doc.relationships.customer.targets &&
		doc.relationships.customer.targets[0];
        var segment = customerRel && 
            		customerRel.extensions &&
            		customerRel.extensions.tenant &&
            		customerRel.extensions.tenant.segment &&
            		customerRel.extensions.tenant.segment.value &&
            		customerRel.extensions.tenant.segment.value.displayName;
        var coveredRel= doc.relationships.covered &&
                	doc.relationships.covered.targets &&
                	doc.relationships.covered.targets[0];
        var customerId = customerRel && customerRel.key;
        var coveredId = coveredRel && coveredRel.key;
        var aeosDate = coveredRel &&
        	coveredRel.extensions &&
        	coveredRel.extensions.tenant &&
        	coveredRel.extensions.tenant.eosDate &&
        	coveredRel.extensions.tenant.eosDate.value;
            aeosDate = isoDate(aeosDate);

        var peosDate = coveredRel && coveredRel.relationships && coveredRel.relationships.product &&
        	coveredRel.relationships.product.targets &&
        	coveredRel.relationships.product.targets[0] &&
        	coveredRel.relationships.product.targets[0].extensions &&
        	coveredRel.relationships.product.targets[0].extensions.tenant &&
        	coveredRel.relationships.product.targets[0].extensions.tenant.eosDate &&
        	coveredRel.relationships.product.targets[0].extensions.tenant.eosDate.value;
            peosDate = isoDate(peosDate);

        var localChannelCode = extValue(doc, 'localChannelCode', false);
        var contractNumber = extValue(doc, 'contractNumber', true);
        var serialNumber = extValue(doc, 'serialNumber', true);
    	var batchQuarter = extValue(doc, 'batchQuarter', true);
    	var shipDate = isoDate(extValue(doc, 'shipDate', false));
    	var serviceClass = extValue(doc, 'serviceClass', false);

    	serviceClass = serviceClass && serviceClass.displayName;

        print(displayName + '\t' + uid + '\t' + primary + '\t' + serialNumber + '\t' + contractNumber + '\t' + endDate + 
    	   '\t' + batchQuarter + '\t' + shipDate + '\t' + segment + '\t' + serviceClass + '\t' + 
    	    aeosDate + '\t' + peosDate + '\t' + customerId + '\t' + coveredId + '\t' + doc.associatedOpportunity);
        });

