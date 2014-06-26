load('../scripts/helper.js');
load('ibmOppNames.js');

var filter = {
    'systemProperties.tenant': 'ibm',
    'systemProperties.expiredOn': ISODate('9999-01-01'),
    displayName: {$in: names}
};

db.app.opportunities.find(filter).addOption(DBQuery.Option.noTimeout).readPref('secondary').forEach(function(doc) {
	var baseQuote = getRelKey(doc, 'baseQuote');
	var primaryQuote = getRelKey(doc, 'primaryQuote');

	var quoteId = primaryQuote ? primaryQuote : baseQuote;
	if (!quoteId ) print ('FAIL on ' + doc.displayName);
	var amt = db.app.quotes.findOne({_id: ObjectId(quoteId)}, {amount: 1}).amount.amount;
	var namt = db.app.quotes.findOne({_id: ObjectId(quoteId)}, {amount: 1}).normalizedAmount.amount;

	//print('Resetting '+ doc.displayName + ' from ' + doc.amount.amount + ' to ' + amt);
	db.app.opportunities.update({_id: doc._id}, {$set: {'amount.amount': amt, 'amount.normalizedAmount.amount': namt}});
});
