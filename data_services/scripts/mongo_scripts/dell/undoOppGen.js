//Params
var tenant = "dell";

//var buId = '808';
//var incomplete = true;
//var deleteOnlyEmpty = true;
//var mockRun = true;
//var qtr = 'FY14Q3';
//var notContacted = false;
//var tags = [];

var filter = {'systemProperties.tenant': tenant, 'systemProperties.expiredOn': ISODate('9999-01-01')};

if (tags && tags.length > 0) filter.tags = {$in: tags};
if (buId) filter['relationships.customer.targets.extensions.tenant.buId.value'] = buId;
if (incomplete) filter.displayName = /NotDetermined$/;
if (qtr) filter['extensions.master.targetPeriod.value.displayName'] = qtr;
if (notContacted) filter['flows.salesStages.state.name'] = 'notContacted';
if (_id && _id != 'false') filter._id = ObjectId(_id);

var i = 0;
var modified = 0;
var expiredDate = new ISODate();
var getRel = function (doc, name) {
  return (doc && doc.relationships && doc.relationships[name] && doc.relationships[name].targets && doc.relationships[name].targets[0] && doc.relationships[name].targets[0].key);
};

printjson(filter);
print ("Expiring records with " + expiredDate);
db.app.opportunities.find(filter).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(function(opp) {
        var offerCount = 0;
        var quotes = [getRel(opp, 'baseQuote'), getRel(opp, 'primaryQuote'), getRel(opp, 'latestQuote'), getRel(opp, 'base'), getRel(opp, 'quote')];
        var filter = {
                'systemProperties.tenant': tenant,
                'systemProperties.expiredOn': ISODate('9999-01-01'),
                'relationships.quote.targets.key': {$in: quotes}
        };


        if (!deleteOnlyEmpty) {
                // Delete the offers
                db.app.offers.find(filter).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(function(offer) {
                        if (offerCount++ % 100 == 0 && offerCount != 1) print('[' + ISODate()+ '] Clean done for ' + offerCount + ' offers');

                        var assetId = getRel(offer, 'predecessor');
                        if(!mockRun && assetId) db.app.assets.update({_id: ObjectId(assetId)}, {$set: {associatedOpportunity: false}});
                        if(!mockRun) db.app.offers.update({_id: offer._id}, {$set: {'systemProperties.expiredOn': expiredDate}});
                });
        }

        if (!deleteOnlyEmpty || (offerCount == 0 && deleteOnlyEmpty) ) {
            modified++;

            // Delete the quotes
            quotes.forEach(function(quoteId) {
                if (!quoteId || !ObjectId(quoteId)) return;
                if(!mockRun) db.app.quote.inverses.remove({_id: ObjectId(quoteId)});
                if(!mockRun) db.app.quotes.update({_id: ObjectId(quoteId)}, {$set: {'systemProperties.expiredOn': ISODate()}});
            });

            // Delete the opportunities
            if(!mockRun) db.app.opportunities.update({_id: opp._id}, {$set: {'systemProperties.expiredOn': expiredDate}} );
        }

        if (i++ % 1000 == 0) print('Scanned with ' + expiredDate + ' for ' + i +  ' opportunities and ' + (mockRun ? 'found' : 'deleted') + ' '  + modified);
});

db.getLastError();
print('Done');
                               
