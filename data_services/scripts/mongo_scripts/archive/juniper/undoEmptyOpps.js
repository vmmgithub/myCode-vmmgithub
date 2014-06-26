//Params
var tenant = "juniper";
var mockRun = false;

var filter = {'systemProperties.tenant': tenant, 'systemProperties.expiredOn': ISODate('9999-01-01')};
var i = 0;
var modified = 0;
var expiredDate = new ISODate();
var getRel = function (doc, name) {
  return (doc && doc.relationships && doc.relationships[name] && doc.relationships[name].targets && doc.relationships[name].targets[0] && doc.relationships[name].targets[0].key);
};

printjson(filter);
print ("Expiring records with " + expiredDate);
db.app.opportunities.find(filter).forEach(function(opp) {
        var offerCount = 0;
        var quotes = [getRel(opp, 'baseQuote'), getRel(opp, 'primaryQuote'), getRel(opp, 'latestQuote'), getRel(opp, 'base'), getRel(opp, 'quote')];
        var filter = {
                'systemProperties.tenant': tenant,
                'systemProperties.expiredOn': ISODate('9999-01-01'),
                'relationships.quote.targets.key': {$in: quotes}
        };

        // Delete the offers
        var c = db.app.offers.count(filter);

        if (c == 0) {
            modified++;

            // Delete the quotes
            quotes.forEach(function(quoteId) {
                if (!quoteId || !ObjectId(quoteId)) return;
                if(!mockRun) db.core.link.inverses.remove({_id: ObjectId(quoteId)});
                if(!mockRun) db.app.quotes.update({_id: ObjectId(quoteId)}, {$set: {'systemProperties.expiredOn': expiredDate}});
            });

            // Delete the opportunities
            if(!mockRun) db.app.opportunities.update({_id: opp._id}, {$set: {'systemProperties.expiredOn': expiredDate}} );        
        }

        if (i++ % 1000 == 0) print('Scanned with ' + expiredDate + ' for ' + i +  ' opportunities and deleted ' + modified);
});

db.getLastError();
print('Done');
