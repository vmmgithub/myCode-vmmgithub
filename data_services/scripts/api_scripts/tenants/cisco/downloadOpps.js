#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var log = console.log;
var jsonpath = require('JSONPath').eval;
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility to export opportunities and its associated data from  quote, offer, booking and asset objects.\
        \n The data can be downloaded using -b or --searchBy option example -b {"_id":"52fe2973386028910a00b858"}\
        \n \
        \n  Note : Add single quote around the curly brackets( {} )   . \
        \n \
        \n The CSV file supports _id, displayName and externalIds.id columns in the file  example :\
        \n _id\
        \n 52fe2973386028910a00b858\
        \n 52fe2973386028910a00b839\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('n', 'port').describe('n', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password')
    .alias('f', 'file').describe('f', 'File Name')
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('d', 'displayName').describe('d', 'Display Name of the opportunity to download')
    .alias('g', 'tags').describe('g', 'Tag associated with the opportunity to download')
    .alias('b', 'searchBy').describe('b', 'Generic filter associated with the opportunities to download. JSON string input required')
    .demand(['h', 't'])
    .argv;

var restApi = h.getAPI(input),
    oppCollection = h.getCollection(restApi, 'app.opportunity'),
    quotesCollection = h.getCollection(restApi, 'app.quote'),
    bookingsCollection = h.getCollection(restApi, 'app.booking'),
    offersCollection = h.getCollection(restApi, 'app.offer'),
    assetsCollection = h.getCollection(restApi, 'app.asset'),
    csvHelper = new csvHelperInstance();

var cols = {
      'opp':["_id",
      "displayName",
      "extensions.tenant.acrID.value",
      "extensions.tenant.acramount.value",
      "targetDate",
      "commitLevel.name",
      "targetAmount",
      "amount",
      "isSubordinate",
      "flows.salesStages.state.name",
      "relationships.customer",
      "relationships.salesRep",
      "relationships.quote",
      "relationships.baseQuote",
      "relationships.primaryQuote",
      "relationships.latestQuote",
      "relationships.booking",
      "extensions.master.batchQuarter.value",
      "extensions.master.clientBatchQuarter.value",
      "extensions.master.targetPeriod.value.name",
      "externalIds.id",
      "systemProperties.createdOn",],
    'qt': [
      "_id",
      "type",
      "displayName",
      "amount",
      "relationships.booking",
      "attachedQuotes.documentName",
      "extensions.tenant.isPrequote.value",
      "flows.quoteStages.state.name"
    ],
    'book': [
      "_id",
      "type",
      "displayName",
      "amount",
    ],
    'off': [
      "_id",
      "type",
      "displayName",
      "amount",
      "extensions.tenant.acramount.value",
      "relationships.product",
      "targetAmount",
      "relationships.predecessor",
      "relationships.quote",
      "resultReason.name",
      "result.name"
    ],
    'ass': [
       "_id",
      "type",
      "displayName",
      "amount",
      "extensions.master.serialNumber.value",
      "externalIds.id",
    ]
};
// Globals
var allColumnNames;
var globalSearchBy;
var globalFields;

var findSource = function (searchBy, value, callback) {
    h.findRecords(oppCollection, {
        multiple: true,
        searchBy: searchBy,
        value: value,
    }, callback);
};

var scanOpps = function (callback) {
    var filter = {};
    if (input.searchBy) {
      try {
        filter = JSON.parse(input.searchBy);
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
        return callback(err, records || []);            
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

var getValue = function(obj, path) {

    if (h.startsWith(path, 'relationships')) {
        var relName = path.split('.')[1];
        var target = h.getRel(obj, relName);
        if (target) 
            return (target.key + '::' + target.displayName);
        else 
            return '::';
    }

    if (h.endsWith(path, 'mount')) {
        var fieldName = path.split('.')[0];
        var amt = obj[fieldName];

        if (amt) 
            return (amt.amount + '::' + (amt.code && amt.code.name));
        else 
            return '::';
    }

    if (path == 'externalIds.id') {
        var ids = _.pluck(obj.externalIds, 'id');
        if (!_.isEmpty(ids)) 
            return _.reduce(ids, function(id, i) { return id + '::' + i});
        else 
            return '';
    }
    return _.flatten(jsonpath(obj, path));
}

var printOpps = function(opp, qt, bk, off, ass) {
    var s = '';
    if (opp) {
        _.each(cols['opp'], function(path) {
            s+= '"' + getValue(opp, path) + '",';
        });
    }

    if (qt) {
        _.each(cols['qt'], function(path) {
            s+= '"' + getValue(qt, path) + '",';
        });
    }

    if (off) {
        _.each(cols['off'], function(path) {
            s+= '"' + getValue(off, path) + '",';
        });
    }

    if (ass) {
        _.each(cols['ass'], function(path) {
            s+= '"' + getValue(ass, path) + '",';
        });
    }

    if (bk) {
        _.each(cols['book'], function(path) {
            s+= '"' + getValue(bk, path) + '",';
        });
    }

    console.log(s);
};

var printHeader = function() {
    var s = '';
    _.each(cols['opp'], function(path) {
        s+= '"opp-' + path + '",';
    });

    _.each(cols['qt'], function(path) {
        s+= '"qt-' + path + '",';
    });

    _.each(cols['off'], function(path) {
        s+= '"off-' + path + '",';
    });

    _.each(cols['ass'], function(path) {
        s+= '"ass-' + path + '",';
    });

    _.each(cols['book'], function(path) {
        s+= '"book-' + path + '",';
    });
    console.log(s);
};    

var processRecord = function (searchBy, searchByValue, callback) {
    var done = function(err) {
        if (err)
            log('error', "Skipping  opportunity '" + searchByValue + "': " + JSON.stringify(err));
        else
            log('info', "Skipping  opportunity '" + searchByValue + "'");

        callback();
    };
    findSource(searchBy.fieldName, searchByValue, function(err, res) {
        if (err) return done(err);
       
       async.eachLimit(res, input.limit, function(opp, cb) {
            downloadOpp(opp, cb);
         },
       done);
     });
}
var getDataType = function(columnName) {
    var datatype ='string';
    var chops = columnName.split('(');
    var fieldName = chops[0];
    if (chops.length > 1)
        datatype = chops[1].split(')')[0];

    return {
        datatype: datatype,
        fieldName: fieldName
    }
}


printHeader();
if (!input.file)  {
   scanOpps(function(err) {
      if (err) console.log('Done ' + err);
   });
};

if (input.file) {

    csvHelper.readAsObj(input.file, function (data) {

    async.eachLimit(data, 5, function (csvRecord, callback) {

        if (!data) return callback();

            if (!allColumnNames) {
                allColumnNames = _.keys(csvRecord);     // contains column names and datatype
                globalSearchBy = getDataType(_.first(allColumnNames));    //  Identify searchBy(first column) from all
            }

            var searchByValue = _.first(_.values(csvRecord));

            if (globalSearchBy.fieldName == '_id' || globalSearchBy.fieldName == 'externalIds.id' || globalSearchBy.fieldName == 'displayName' ) 
                processRecord(globalSearchBy, searchByValue, callback);
            else
                {
                log('Skipping ' + csvRecord);
                callback();
            }
    },
       function (err) {
           console.log('info', "DONE " + err);
       });
   });
}