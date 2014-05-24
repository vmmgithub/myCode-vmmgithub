rs.slaveOk();
load('../common/helper.js');

var i = 0;
var updated = 0;

var printContact = function(doc) {
   if (i++ == 100) print('Processed ' + i + ' and updated ' + updated + ' for bad serviceClass' );

   var product = getRel(doc, 'product');
   var predecessor = getRel(doc, 'predecessor');

   if (!product) return;

   var serviceClass = product.extensions && product.extensions.tenant && product.extensions.tenant.serviceClass && product.extensions.tenant.serviceClass.value && product.extensions.tenant.serviceClass.value.displayName;

   if (!serviceClass && product.key) {
	var cproduct = db.app.products.find({_id: ObjectId(product.key)}).limit(1);
	product = cproduct.hasNext() && cproduct.next();
	if(product) serviceClass = product.extensions && product.extensions.tenant && product.extensions.tenant.serviceClass && product.extensions.tenant.serviceClass.value && product.extensions.tenant.serviceClass.value.displayName
   }

   if (!serviceClass && predecessor.key) {
	var cass = db.app.assets.find({_id: ObjectId(predecessor.key)}).limit(1);
	var ass = cass.hasNext() && cass.next();
	product =  getRel(ass, 'product');

	if (product.key) {
		var cproduct = db.app.products.find({_id: ObjectId(product.key)}).limit(1);
		product = cproduct.hasNext() && cproduct.next();
		if(product) serviceClass = product.extensions && product.extensions.tenant && product.extensions.tenant.serviceClass && product.extensions.tenant.serviceClass.value && product.extensions.tenant.serviceClass.value.displayName
	}
   }
   
   updated++;
   var newName = doc.displayName.replace('Unknown Service Class', serviceClass);
   doc.displayName = newName;
   db.app.offers.save(doc);
}

var filter = {
	"systemProperties.tenant": "dell",
	"systemProperties.expiredOn": ISODate("9999-01-01T00:00:00Z"),
	displayName: /Unknown Service Class/
};
var getClause = {
	'relationships.product': 1, 
	'relationships.predecessor': 1, 
	displayName: 1
};

db.app.offers.find(filter).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(printContact);
