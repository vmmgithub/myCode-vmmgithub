#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility to export for any object in Renew, using a CSV input.\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('n', 'port').describe('n', 'Specify port')
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password')
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('c', 'columns').describe('c', 'List of columns for download in the dot notation. Prefix object type (opp, qt, ass, book, off) followed by JSON path (Ex. opp:resolutionDate)')
    .alias('d', 'displayName').describe('d', 'Display Name of the opportunity to download')
    .alias('g', 'tags').describe('g', 'Tag associated with the opportunity to download')
    .alias('a', 'addDefaultColumns').describe('a', 'Adds default columns to the output').boolean('a').default('a', true)
    .alias('f', 'filter').describe('f', 'Generic filter associated with the opportunities to download. JSON string input required')
    .demand(['h', 't'])
    .argv;

var restApi = h.getAPI(input),
    oppCollection = h.getCollection(restApi, 'app.opportunity'),
    quotesCollection = h.getCollection(restApi, 'app.quote'),
    bookingsCollection = h.getCollection(restApi, 'app.booking'),
    linesCollection = h.getCollection(restApi, 'app.lineitems'),
    offersCollection = h.getCollection(restApi, 'app.offer'),
    assetsCollection = h.getCollection(restApi, 'app.asset');

var cols = {
    'opp': [
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
      "relationships.customer.targets.keyNameType",
      "relationships.salesRep.targets.keyNameType",
      "relationships.quote.targets.keyNameType",
      "relationships.baseQuote.targets.keyNameType",
      "relationships.primaryQuote.targets.keyNameType",
      "relationships.booking.targets.keyNameType",
      "extensions.master.batchQuarter.value",
      "extensions.master.clientBatchQuarter.value",
      "extensions.master.targetPeriod.value.name",
      "externalIds.id",
      "systemProperties.createdOn",
    ],
    'qt': [
      "_id",
      "type",
      "displayName",
      "amount.amount",
      "amount.code.name",
      "relationships.booking.targets.keyNameType"
    ],
    'book': [
      "_id",
      "type",
      "displayName",
      "amount.amount",
      "amount.code.name",
      "poAmount.amount",
      "poAmount.code.name",
      "soAmount.amount",
      "soAmount.code.name",
    ],
    'off': [
      "_id",
      "type",
      "displayName",
      "amount.amount",
      "amount.code.name",
      "targetAmount.amount",
      "targetAmount.code.name",
      "relationships.predecessor.targets.keyNameType",
      "relationships.quote.targets.keyNameType",
      "resultReason.name",
      "result.name",
      "startDate",
      "endDate",
    ],
    'ass': [
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
    'items': [
      "_id",
      "type",
      "displayName",
      "amount.amount",
      "amount.code.name",
      "relationships.predecessor.targets.keyNameType",
      "relationships.base.targets.keyNameType",
      "headerDocument.headerKey.targets.keyNameType",
    ]
};

// Will clone the object for output. This way the script still downloads all
// needed values for later processing, but the output is only the
// requested columns.

var colsoutput = JSON.parse(JSON.stringify(cols));

if (!input.addDefaultColumns) {
  colsoutput.opp = ['_id'];
  colsoutput.qt = ['_id'];
  colsoutput.book = ['_id'];
  colsoutput.off = ['_id'];
  colsoutput.ass = ['_id'];
  colsoutput.items = ['_id'];
}

if (!_.isEmpty(input.columns)) {
  if (_.isString(input.columns)) input.columns = [input.columns];

  _.each(input.columns, function(input) {
    var pre = input.split(':')[0];
    var po = input.split(':')[1];

    switch (pre) {
      case 'opp': cols.opp.push(po); colsoutput.opp.push(po); break;
      case 'qt': cols.qt.push(po); colsoutput.qt.push(po); break;
      case 'book': cols.book.push(po); colsoutput.book.push(po); break;
      case 'off': cols.off.push(po); colsoutput.off.push(po); break;
      case 'ass': cols.ass.push(po); colsoutput.ass.push(po); break;
      case 'items': cols.items.push(po); colsoutput.items.push(po); break;
    }

  });
}

// Renew does not understand the keyname type concept, so lets remove it
_.each(_.keys(cols), function(coll) {
  cols[coll] = _.map(cols[coll], function(path) { 
    if (h.startsWith(path, 'relationships'))
      return 'relationships.' + path.split('.')[1];
    else 
      return path;
  });
});

var scanOpps = function (callback) {
    var filter = {};
    if (input.filter) {
      try {
        filter = JSON.parse(input.filter);
      } catch(err) {
        h.log('error', err);
      }
    } else {
      if (input.displayName) filter.displayName = input.displayName;
      if (input.tags) filter.tags = input.tags;
    }

    h.findRecords(oppCollection, {
        filter: filter,
        stream: true,
        columns: cols['opp']
    }, callback, downloadOpp);
};

var downloadOpp = function(opp, callback) {
  async.auto({
      getQuotes: function(cb) {
          scanQuotes(opp, cb);
      },
      getBookings: function(cb) {
          scanBookings(opp, cb);
      },
      getOffers: ['getQuotes', function(cb, res) {
          scanOffers(res.getQuotes, cb);
      }],
      getAssets: ['getOffers', function(cb, res) {
          scanAssets(res.getOffers, cb);
      }],
      printEverything: ['getQuotes', 'getOffers', 'getAssets', 'getBookings', function(cb, res) {

        //Step 0: prepare results for easier print
        joinResults(res);

        //Scenario 1: Empty opportunity without any quotes, offers, bookings
        if (_.isEmpty(res.getOffers) && _.isEmpty(res.getQuotes) && _.isEmpty(res.getBookings)) {
          printOpps(opp);
          return callback && callback();
        }

        //Scenario 2: Unreferenced quotes exist on an opp without any offers, bookings
        //print separate lines for each of them
        if (!_.isEmpty(res.getQuotes)) {
          _.each(res.getQuotes, function(quote) {
            printOpps(opp, quote, quote.booking);
          });
        }

        //Scenario 3: Unreferenced quote and bookings without any offers
        //print separate lines for each of them
        if (!_.isEmpty(res.getBookings)) {
          _.each(res.getBookings, function(booking) {
            printOpps(opp, null, booking);
          });
        }

        //Scenario 4: Print offers with everything you can
        if (!_.isEmpty(res.getOffers)) {
          _.each(res.getOffers, function(offer) {
              printOpps(opp, offer.quote, offer.quote.booking, offer, offer.asset);
          });
          return callback && callback();
        }

        cb();
      }],
  }, function(err) {
    callback && callback(err);
  });
};

var joinResults = function(res) {

  var scanAndMarkRel = function(arr, key) {
    if (!key) return;
    return _.find(arr, function(a) { if (a._id == key) { a.referenced = true; return true;} return false; });
  };

    //1 Associate the related objects for quotes, using keys
    _.each(res.getQuotes, function(quote) {
      quote.booking = scanAndMarkRel(res.getBookings, h.getRelKey(quote, 'booking'));
    });

    //2 Associate the related objects for offers, using keys
    _.each(res.getOffers, function(offer) {
      offer.asset = scanAndMarkRel(res.getAssets, h.getRelKey(offer, 'predecessor'));
      offer.quote = scanAndMarkRel(res.getQuotes, h.getRelKey(offer, 'quote'));
    });

    //3 Remove referenced documents and only leave out unreferenced danglers
    res.getQuotes = _.reject(res.getQuotes, function(a){ return a.referenced; });
    res.getBookings = _.reject(res.getBookings, function(a){ return a.referenced; });
    res.getAssets = _.reject(res.getAssets, function(a){ return a.referenced; });

    return res;
};

var scanBookings = function (opp, callback) {
    var bookingIds = h.getRelKeys(opp, 'booking');
    if (_.isEmpty(bookingIds)) return callback(null, []);

    bookingsCollection.find({_id: {$in: bookingIds}}, {columns: cols['book']}, function(err, records) {
        if (err || !records || _.isEmpty(records)) callback(err, records || []);

        linesCollection.find({type: 'app.lineitem/booking', 'headerDocument.headerKey': {$in: bookingIds}}, {columns: cols['items']}, function(err, items) {
          if (err || !items || _.isEmpty(items)) return callback(err, records);

          _.each(items, function(item) {
            var b = _.find(records, function(book){ return book._id == item.headerDocument.headerKey});
            if (!b) return;

            if (!b.items) b.items = [];
            b.items.push(item);
          });

          return callback(null, records);
        });
    });
};

var scanQuotes = function (opp, callback) {
    var quoteIds = _.union(h.getRelKeys(opp, 'baseQuote'), h.getRelKeys(opp, 'quote'), h.getRelKeys(opp, 'primaryQuote'), h.getRelKeys(opp, 'latestQuote'));

    if (_.isEmpty(quoteIds)) return callback(null, []);

    quotesCollection.find({_id: {$in: quoteIds}}, {columns: cols['qt']}, function(err, records) {
        return callback(err, records || []);
    });
};

var scanOffers = function (quotes, callback) {
    if (_.isEmpty(quotes)) return callback(null, []);

    var qs = _.pluck(quotes, '_id');
    offersCollection.find({'relationships.quote.targets.key': {$in: qs}}, {columns: cols['off']}, function(err, records) {
        return callback(err, records || []);
    });
};

var scanAssets = function (offers, callback) {
    if (_.isEmpty(offers)) return callback(null, []);

    var assetIds = [];
    _.each(offers, function(off) {
      assetIds.push(h.getRelKeys(off, 'predecessor'));
    });
    assetIds = _.uniq(_.compact(_.flatten(assetIds)));

    assetsCollection.find({_id: {$in: assetIds}}, {columns: cols['ass']}, function(err, records) {
        return callback(err, records || []);
    });
};

var printOpps = function(opp, qt, bk, off, ass) {
    var s = '';
    if (opp) {
        _.each(colsoutput['opp'], function(path) {
            s+= '"' + h.getObjectValueFromPath(opp, path) + '",';
        });
    } else {
        _.each(colsoutput['opp'], function(path) {
            s+= '"",';
        });
    }

    if (qt) {
        _.each(colsoutput['qt'], function(path) {
            s+= '"' + h.getObjectValueFromPath(qt, path) + '",';
        });
    } else {
        _.each(colsoutput['qt'], function(path) {
            s+= '"",';
        });
    }

    if (off) {
        _.each(colsoutput['off'], function(path) {
            s+= '"' + h.getObjectValueFromPath(off, path) + '",';
        });
    } else {
        _.each(colsoutput['off'], function(path) {
            s+= '"",';
        });
    }

    if (ass) {
        _.each(colsoutput['ass'], function(path) {
            s+= '"' + h.getObjectValueFromPath(ass, path) + '",';
        });
    } else {
        _.each(colsoutput['ass'], function(path) {
            s+= '"",';
        });
    }

    if (bk) {
        _.each(colsoutput['book'], function(path) {
            s+= '"' + h.getObjectValueFromPath(bk, path) + '",';
        });

        if (bk.items && !_.isEmpty(bk.items)) {
            _.each(colsoutput['items'], function(path) {
                s+= '"' + h.getObjectValueFromPath(bk.items, path) + '",';
            });
        } else {
            _.each(colsoutput['items'], function(path) {
                s+= '"",';
            });
        }
    } else {
        _.each(colsoutput['book'], function(path) {
            s+= '"",';
        });
        _.each(colsoutput['items'], function(path) {
            s+= '"",';
        });
    }

    console.log(s);
};

var printHeader = function() {
    var s = '';
    _.each(colsoutput['opp'], function(path) {
        s+= '"opp-' + path + '",';
    });

    _.each(colsoutput['qt'], function(path) {
        s+= '"qt-' + path + '",';
    });

    _.each(colsoutput['off'], function(path) {
        s+= '"off-' + path + '",';
    });

    _.each(colsoutput['ass'], function(path) {
        s+= '"ass-' + path + '",';
    });

    _.each(colsoutput['book'], function(path) {
        s+= '"book-' + path + '",';
    });

    _.each(colsoutput['items'], function(path) {
        s+= '"book-items-' + path + '",';
    });

    //s+= '"book-items",';
    console.log(s);
};

printHeader();
scanOpps(function(err) {
    if (err) h.log('error ', err);
});

