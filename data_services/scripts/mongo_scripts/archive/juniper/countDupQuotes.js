var i = 0;
var tenant="juniper";
var coll = "app.opportunities";
load('../common/helper.js');

var getLookup = function(name, core, group) {
        var coll = core ? "core.lookups": "app.lookups";
	var filter = {"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), displayName: name};
	if (group) filter.group = group;

        var lkp = db[coll].findOne(filter);
        if (lkp) {
                var res = {name: lkp.name, displayName: lkp.displayName, type: lkp.type, key: lkp._id.valueOf()};
                return res;
        }
};

var quotes = {};

var addRef = function(oppId, quoteId, relType) {
    if (!oppId || !quoteId) return;
	
    if (!quotes[quoteId]) quotes[quoteId] = {ref: 0};
    quotes[quoteId].ref++;
    if (!quotes[quoteId][relType]) quotes[quoteId][relType] = [];
    quotes[quoteId][relType].push(oppId);
}

var rels = ['baseQuote', 'quote', 'primaryQuote', 'latestQuote'];
db.app.opportunities.find({"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), isSubordinate: false}).forEach(function(opp) {
  var o = _id(opp);
  o = opp.isSubordinate ? o + "-S" : o;

  rels.forEach(function(rel) {
     var ids = getRelKeys(opp, rel);
     if (ids && ids.length > 0) {
	ids.forEach(function(id) { addRef(o, id, rel); });
     }
  });
});

for (var q in quotes) {
  if (quotes[q].ref == 1) delete quotes[q];
  else if (quotes[q].ref == 2 && quotes[q].quote && quotes[q].quote.length == 1 && quotes[q].primaryQuote && quotes[q].primaryQuote.length == 1 && quotes[q].quote[0] == quotes[q].primaryQuote[0]) delete quotes[q];
  else if (quotes[q].ref == 2 && quotes[q].quote && quotes[q].quote.length == 1 && quotes[q].latestQuote && quotes[q].latestQuote.length == 1 && quotes[q].quote[0] == quotes[q].latestQuote[0]) delete quotes[q];
}

printjson(quotes);
