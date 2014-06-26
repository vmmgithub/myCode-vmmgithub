var extValue = function (doc, name, isMaster) {
  var e = isMaster ? 'master': 'tenant';
  if (doc && doc.extensions && doc.extensions[e] && doc.extensions[e][name] && doc.extensions[e][name].value) {
    return (doc.extensions[e][name].value.name || doc.extensions[e][name].value.displayName || doc.extensions[e][name].value);
  } else {
    return '';
  }
};

var getRel = function (doc, name) {
  return (doc && doc.relationships && doc.relationships[name] && doc.relationships[name].targets && doc.relationships[name].targets[0]);
};

var filter = {"systemProperties.tenant": "ibm", "systemProperties.expiredOn": ISODate("9999-01-01")};
var i = 0;

db.app.opportunities.find(filter).forEach(function(opp) {

	var baseQuote = getRel(opp, 'baseQuote') && getRel(opp, 'baseQuote').key;
	var primaryQuote = getRel(opp, 'primaryQuote') && getRel(opp, 'primaryQuote').key;
	var latestQuote = getRel(opp, 'latestQuote') && getRel(opp, 'latestQuote').key;
	var base = getRel(opp, 'base') && getRel(opp, 'base').key;

	var quotes = [baseQuote, primaryQuote, latestQuote, base];
	var filter = {'systemProperties.tenant': 'ibm', 'systemProperties.expiredOn': ISODate('9999-01-01'), 'relationships.quote.targets.key': {$in: quotes}};

	var mixed = false;
	var prev = "first";
	db.app.offers.find(filter).forEach(function(offer) {
		var oType = extValue(offer, 'contractType', false);
		oType = (oType == 'autorenewal'? 'evergreen': 'regular'); 
		if (prev != "first" && prev != oType) mixed = true; 
		prev = oType;
	});

	if (mixed) 
		//printjson(filter); 
		db.app.opportunities.update({_id: opp._id}, {$push: {'tags': 'MixedContractType'}} );

});

print (i);
