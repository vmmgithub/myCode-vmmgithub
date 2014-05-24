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
        // Delete the offers
        db.app.offers.find(filter).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(function(offer) {
            if (offerCount++ % 100 == 0 && offerCount != 1) print('[' + ISODate()+ '] Clean done for ' + offerCount + ' offers');
        });

        if (i++ % 10 == 0) print('Scanned with ' + expiredDate + ' for ' + i +  ' opportunities and ' + (mockRun ? 'found' : 'deleted') + ' '  + modified);
});

db.getLastError();
print('Done');
