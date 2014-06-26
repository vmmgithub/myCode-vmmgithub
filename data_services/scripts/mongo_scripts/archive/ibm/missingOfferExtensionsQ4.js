load('../common/helper.js');

var filter = {
    'systemProperties.tenant': 'ibm',
    'systemProperties.expiredOn': ISODate('9999-01-01'),
//    'systemProperties.createdOn': {$gt: ISODate('2013-09-27')},
};

var extensions = [
	{name: 'contractType', type: 'tenant', model: 'app.asset/service'}, 
	{name: 'BillCustomerNumber', type: 'tenant', model: 'app.asset/service'},
	{name: 'country', type: 'master', model: 'app.asset/service'}, 
	{name: 'contractNumber', type: 'master', model: 'app.asset/service'}, 
	{name: 'customerNumber', type: 'tenant', model: 'core.contact/organization'}, 
];


db.app.offers.find(filter).forEach(function(offer) {
	var assetKey = getRelKey(offer, 'predecessor');
	var customerKey = getRelKey(offer, 'customer');
	var modified = false;

	if (!assetKey || !customerKey) { 
		print ('Skipping ' + offer._id.valueOf()); 
		return; 
	}
	
	var asset = db.app.assets.findOne({_id: ObjectId(assetKey)});
	var customer = db.core.contacts.findOne({_id: ObjectId(customerKey)});

	if (!asset || !customer) { 
		print ('Skipping 2' + offer._id.valueOf()); 
		return; 
	}

	extensions.forEach(function(ext) {
		if (ext.model == 'app.asset/service') {
			var existingOfferExt = extValue(offer, ext.name, ext.type == 'master');
			var existingAssetExt = extValue(asset, ext.name, ext.type == 'master');

			if (existingAssetExt != existingOfferExt) {
		print('changing asset ext ' + ext.name + ' from ' + existingOfferExt +  ' to ' + existingAssetExt);
				if (!offer.extensions[ext.type]) offer.extensions[ext.type] = {};
				
				offer.extensions[ext.type][ext.name] = asset.extensions[ext.type][ext.name];
				modified = true;
			}
		}

		if (ext.model == 'core.contact/organization') {
			var existingOfferExt = extValue(offer, ext.name, ext.type == 'master');
			var existingCustomerExt = extValue(customer, ext.name, ext.type == 'master');

			if (existingCustomerExt != existingOfferExt) {
		print('changing customer ext ' + ext.name + ' from ' + existingOfferExt +  ' to ' + existingCustomerExt);
				if (!offer.relationships.customer.targets[0].extensions) offer.relationships.customer.targets[0].extensions = {};
				if (!offer.relationships.customer.targets[0].extensions[ext.type]) offer.relationships.customer.targets[0].extensions[ext.type] = {};

				offer.relationships.customer.targets[0].extensions[ext.type][ext.name] = customer.extensions[ext.type][ext.name];
				modified = true;
			}
		}
	});

	if (modified) {
		print('Saving ' + offer.displayName);
		db.app.offers.save(offer);
	} else {
		print('No changes on ' + offer.displayName);
	}

});

