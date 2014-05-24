rs.slaveOk();

var i = 0;
var updated = 0;
var shouldBe = 0;

load('../common/helper.js');

var calcEOSDate = function(doc) {

	if (i++ % 1000 == 0) print('[' + ISODate()+ '] Setting done with for ' + i + ' records and updated ' + updated + ' but should have updated ' + shouldBe);    

	var eosDate = extValue(doc, 'eosDate', false);
	if (eosDate) return;

	shouldBe++; 
	var shipDate = doc.extensions && doc.extensions.tenant && doc.extensions.tenant.shipDate && doc.extensions.tenant.shipDate.value;
	var product = getRel(doc, 'product');
	var systemType = product && extValue(product, 'systemType', false);

	if (!systemType || !shipDate) return;

    var eos = new Date(shipDate.getTime());
    var yrs = (systemType === 'enterprise' || systemType === 'Enterprise Products') ? 7 : 5;
    eos.setFullYear(eos.getFullYear() + yrs);

    updated++;
    db.app.assets.update({_id: doc._id}, {$set: {'extensions.tenant.eosDate': {type: 'date', value: eos}}});
}

var getClause = {
    "displayName": 1,
    "extensions": 1,
    "relationships.product.targets" : 1,
    'externalIds.id': 1,
    "systemProperties": 1
    };

var filter = {
	'relationships.customer.targets.extensions.tenant.buId.value': buId,
	"systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
	"systemProperties.tenant": 'dell',
	type: 'app.asset/covered',
};

db.app.assets.find(filter, getClause)
.readPref('secondary')
.addOption(DBQuery.Option.noTimeout)
.forEach(calcEOSDate);

