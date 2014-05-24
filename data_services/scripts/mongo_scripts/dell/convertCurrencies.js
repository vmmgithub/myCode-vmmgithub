var conversionDate = new ISODate();
var differentOnly = true;
var tenant = 'dell';

var convRates = {
};

db.core.currency.rates.find({
		'systemProperties.tenant': tenant,
		'systemProperties.expiredOn': ISODate('9999-01-01'),
		endDate: {$exists: 0}
	}).forEach(function(r) {
		convRates[r.localCurrency.name] = r.exchangeRate
	});

function checkPrecision(val, base) {
	val = Math.round(Math.abs(val));
	return isNaN(val)? base : val;
}

var unformat = function(value, decimal) {

	value = value || 0;
	if (typeof value === "number") return value;

	// Default decimal point is "." but could be set to eg. "," in opts:
	decimal = decimal || ".";

	 // Build regex to strip out everything except digits, decimal point and minus sign:
	var regex = new RegExp("[^0-9-" + decimal + "]", ["g"]),
		unformatted = parseFloat(
			("" + value)
			.replace(/\((.*)\)/, "-$1") // replace bracketed values with negatives
			.replace(regex, '')         // strip out any cruft
			.replace(decimal, '.')      // make sure decimal point is standard
		);

	// This will fail silently which may cause trouble, let's wait and see:
	return !isNaN(unformatted) ? unformatted : 0;
};

var toFixed = function(value, precision) {
	precision = checkPrecision(2, 2);
	var power = Math.pow(10, precision);

	// Multiply up by precision, round accurately, then divide and use native toFixed():
	return (Math.round(unformat(value) * power) / power).toFixed(precision);
};

var covertCurr = function(obj, attr) {
	if (!obj || !attr || !obj[attr] || !obj[attr].amount || obj[attr].amount == 0 || obj[attr].code.name == 'usd') return false;
	if (!obj[attr].normalizedAmount ) return false;
	if (differentOnly && obj[attr].amount != obj[attr].normalizedAmount.amount) return false;

	var rate = convRates[obj[attr].code.name];
	obj[attr].normalizedAmount.amount = parseFloat(toFixed(obj[attr].amount / rate));
	obj[attr].normalizedAmount.convertedOn = conversionDate;

	return true;
}

var collectionAttrs = [
	//{name: 'app.opportunities', values: ['amount', 'targetAmount', 'upsellAmount', 'resellerAmount', 'resellerTargetAmount']},
	//{name: 'app.quotes', values: ['amount', 'targetAmount','upsellAmount','resellerAmount','resellerTargetAmount']},
	//{name: 'app.bookings', values: ['amount', 'poAmount', 'soAmount','resellerAmount','resellerPoAmount']},
	{name: 'app.offers',values:['amount','targetAmount','upsellAmount','resellerAmount','resellerTargetAmount']},
];

var buIds = [/*'7460', '1717', '7465', '340434',*/ '410434', '4065', '414065', '1401', '411401', '4545', '344545', '414545', '4046', '344046', '414046', '4444', '414444', '1313', '341313', '4013', '340439', '340443', '340442', '340000', '3535', '348270', '418270', '8270', '8270',];

buIds.forEach(function(buId) {
	var filter = {
		'systemProperties.tenant': 'dell',
		'systemProperties.expiredOn': ISODate('9999-01-01'),
		'relationships.customer.targets.extensions.tenant.buId.value': buId,
	};

	collectionAttrs.forEach(function(collectionAttr) {
		var i = 0;
		var changed = 0;
		var cols = [];
		collectionAttr.values.forEach(function(v) {cols[v] = 1;});
		db[collectionAttr.name].find(filter, cols).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(function(obj) {
			if (i++ % 1000 == 0) print('[' + ISODate()+ '] Setting done with ' + i + ' for ' + buId + ' and ' + collectionAttr.name + ' changed ' + changed);
			var modified;

			collectionAttr.values.forEach(function(v) { 
				if(covertCurr(obj, v)) {
					if (!modified) modified = {};
					modified[v] = obj[v];
				}
			});

			if (modified) {
				changed++ ;
				db[collectionAttr.name].update({_id: obj._id}, {$set: modified});
			}
		});

		print('[' + ISODate() + '] Done with ' + buId + ' and ' + collectionAttr.name);
	});

	print('[' + ISODate() + '] Done with ' + buId);
});

db.getLastError();
print('[' + ISODate() + '] Done with everything ');

