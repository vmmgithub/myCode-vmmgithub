load('../common/helper.js');

var filter = {
    'systemProperties.tenant': 'ibm',
    'systemProperties.expiredOn': ISODate('9999-01-01'),
//    'systemProperties.createdOn': {$gt: ISODate('2013-09-27')},
};

var extensions = [
	{name: 'customerNumber', type: 'tenant', model: 'core.contact/organization'}, 
];


db.app.opportunities.find(filter).forEach(function(opportunity) {
	var customerKey = getRelKey(opportunity, 'customer');
	var modified = false;

	if (!customerKey) { 
		print ('Skipping ' + opportunity._id.valueOf()); 
		return; 
	}
	
	var customer = db.core.contacts.findOne({_id: ObjectId(customerKey)});

	if (!customer) { 
		print ('Skipping 2' + opportunity._id.valueOf()); 
		return; 
	}

	extensions.forEach(function(ext) {

		if (ext.model == 'core.contact/organization') {
			var existingopportunityExt = extValue(opportunity, ext.name, ext.type == 'master');
			var existingCustomerExt = extValue(customer, ext.name, ext.type == 'master');

			if (existingCustomerExt != existingopportunityExt) {
//		print('changing customer ext ' + ext.name + ' from ' + existingopportunityExt +  ' to ' + existingCustomerExt);
				if (!opportunity.relationships.customer.targets[0].extensions) opportunity.relationships.customer.targets[0].extensions = {};
				if (!opportunity.relationships.customer.targets[0].extensions[ext.type]) opportunity.relationships.customer.targets[0].extensions[ext.type] = {};

				opportunity.relationships.customer.targets[0].extensions[ext.type][ext.name] = customer.extensions[ext.type][ext.name];
				modified = true;
			}
		}
	});

	if (modified) {
		print('Saving ' + opportunity.displayName);
		db.app.opportunities.save(opportunity);
	} else {
		print('No changes on ' + opportunity.displayName);
	}

});

