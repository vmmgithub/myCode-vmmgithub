load('./underscore.js');

var COLL_LOOKUP = {
    'app.opportunities': [
      "_id",
      "displayName",
      "targetDate",
      "commitLevel.name",
      "amount.amount",
      "amount.code.name",
      "targetAmount.amount",
      "targetAmount.code.name",
      "isSubordinate",
      "flows.salesStages.state.name",
      "relationships.customer.targets",
      "relationships.salesRep.targets",
      "relationships.quote.targets",
      "relationships.baseQuote.targets",
      "relationships.primaryQuote.targets",
      "relationships.booking.targets",
      "extensions.master.batchQuarter.value",
      "extensions.master.clientBatchQuarter.value",
      "extensions.master.targetPeriod.value.name",
      "externalIds.id",
      "systemProperties.createdOn",
    ],
    'app.quotes': [
      "_id",
      "type",
      "displayName",
      "amount.amount",
      "amount.code.name",
      "flows.quoteStages.state.name",
      "relationships.booking.targets",
    ],
    'app.bookings': [
      "_id",
      "type",
      "displayName",
      "amount.amount",
      "amount.code.name",
      "poAmount.amount",
      "poAmount.code.name",
      "soAmount.amount",
      "soAmount.code.name",
      "poDate",
      "soDate",
      "flows.bookingStages.state.name",
    ],
    'app.offers': [
      "_id",
      "type",
      "displayName",
      "amount.amount",
      "amount.code.name",
      "targetAmount.amount",
      "targetAmount.code.name",
      "relationships.predecessor.targets",
      "relationships.quote.targets",
      "relationships.product.targets",
      "resultReason.name",
      "result.name",
      "startDate",
      "endDate",
    ],
    'app.assets': [
       "_id",
      "type",
      "displayName",
      "amount.amount",
      "amount.code.name",
      "extensions.master.serialNumber.value",
      "externalIds.id",
      "startDate",
      "endDate",
    ],
    'app.lineitems': [
      "_id",
      "type",
      "displayName",
      "amount.amount",
      "amount.code.name",
      "relationships.predecessor.targets",
      "relationships.base.targets",
      "headerDocument.headerKey",
    ]
};

var cols = COLL_LOOKUP[coll];

function sqlize(str) {
    if (!str) return;
    return str.toUpperCase().replace(/\./g,'_').replace(/[^\w\s]/gi, '');
};

function sqlizeTable(str) {
    if (_.isEmpty(str)) return;
    var n = str.split('/')[0];

    switch (n) {
        case 'app.opportunity': return 'APP_OPPORTUNITIES'; break;
        case 'core.contact': return 'CORE_CONTACTS'; break;
        case 'app.asset': return 'APP_ASSETS'; break;
        case 'app.offer': return 'APP_OFFERS'; break;
        case 'app.quote': return 'APP_QUOTES'; break;
        case 'app.booking': return 'APP_BOOKINGS'; break;
        case 'app.product': return 'APP_PRODUCTS'; break;
        case 'app.lineitem': return 'APP_LINEITEMS'; break;
    };
    return sqlize(str);
};

function startsWith(str, prefix) {
    return str.indexOf(prefix) == 0;
};

function printHeader() {
    var s = ''; 
    _.each(cols, function(col) {
        if (!startsWith(col, 'relationships')) s+= sqlize(col) + ',';
    });
    print(s);
};

function getValue(obj, path) {
    if (!obj) return;
    if (_.isString(obj)) return obj;

    var elems = path.split('.');
    var curr = obj;

    for(var i=0; i < elems.length; i++) {
        var elem = elems[i];
        if (!curr[elem]) return;

        if (_.isArray(curr[elem])) {
            if (_.isEmpty(curr[elem])) return [];

            var ret = [];
            _.each(curr[elem], function(ele) {
                ret.push(getValue(ele, _.reduce(_.rest(elems, i+1), function(str, c) { if(str == '') return c; else return str + '.' + c}, '')));
            });
            return ret;
        }

        if (elem == '_id') return curr[elem].valueOf();
        if (_.isDate(curr[elem])) return curr[elem].toISOString();

        curr = curr[elem];
        if (i == elems.length-1) return curr;
    }
};

var REFS_COLS = ['sourceTable', 'sourceKey', 'destTable', 'destKey', 'destName', 'relName'];
function getRelationship(doc, relation) {
    if (!doc || !doc.relationships || !doc.relationships[relation] || !doc.relationships[relation].targets) return;
    var type = _.first(_.compact(_.pluck(doc.relationships[relation].targets, 'type') || []) || []);
    _.each(doc.relationships[relation].targets, function(target) {
        print('RELATIONSHIPROWS|"' + sqlizeTable(coll) + '","' + getValue(doc, '_id') + '","' + sqlizeTable(type) + '","' + target.key + '","' + target.displayName + '","' + relation + '"'); 
    });
};

function printRelationships(doc) {
   _.each(cols, function(col) {
      if (startsWith(col, 'relationships')) {
        getRelationship(doc, col.split('.')[1]);
      }
   });
};

var printDoc = function(doc) {
    var s = '';
   _.each(cols, function(col) {
      if (!startsWith(col, 'relationships')) s += '"' + getValue(doc, col) + '",';
   });
   printRelationships(doc);
   print(s);
};

var columns = {};
_.each(cols, function(col) {
    columns[col] = 1;
});

printHeader();
db[coll].find({
    'systemProperties.tenant' : tenant,
    'systemProperties.expiredOn' : ISODate('9999-01-01:00:00:00Z'),
}, columns)
.readPref('secondary')
.addOption(DBQuery.Option.noTimeout)
//.limit(5)
.forEach(printDoc);

db.getLastError();
