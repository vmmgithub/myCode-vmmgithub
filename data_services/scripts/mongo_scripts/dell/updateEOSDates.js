var filter = {
	'systemProperties.tenant': 'dell', 
	'systemProperties.expiredOn': ISODate('9999-01-01T00:00:00Z'),
	type: "app.asset/covered", 
	$or: [
		{"relationships.product.targets.key": {$in: keys}},
		{"relationships.product.targets.id": {$in: ids}}
	],
	'extensions.tenant.buId.value': {$ne: '909'}
};

var i = 0;
var updated = 0;
db.app.assets.find(filter).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(function(asset){
	var shipDate = asset.extensions.tenant && asset.extensions.tenant.shipDate && asset.extensions.tenant.shipDate.value;
	var eosDate = asset.extensions.tenant && asset.extensions.tenant.eosDate && asset.extensions.tenant.eosDate.value;	
	var shouldBe = shipDate;
	if (shouldBe) shouldBe.setFullYear(shouldBe.getFullYear() + 7);

  if (i++ % 1000 == 0) print('[' + ISODate() + '] Processed ' + i +' records with ' + updated);
	if (shouldBe && shouldBe.getTime() != eosDate.getTime()) {
		updated++;
		asset.extensions.tenant.eosDate.value = shouldBe;
		db.app.assets.save(asset);
	}
});
