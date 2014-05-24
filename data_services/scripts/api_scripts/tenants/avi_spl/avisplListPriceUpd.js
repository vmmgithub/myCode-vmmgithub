#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('../../common/js/helper');
var log = console.log;
var jsonpath = require('JSONPath').eval;
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility to sum listPrice and priorRenewalNotAnnualizedAmount columns from offer and update into  opportunity \
        \n collection object in respective columns \
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
    .alias('l', 'limit').describe('l', 'Concurrent threads').default ('l', 5)
    .alias('d', 'displayName').describe('d', 'Display Name of the opportunity to download')
    .alias('g', 'tags').describe('g', 'Tag associated with the opportunity to download')
    .alias('b', 'searchBy').describe('b', 'Generic filter associated with the opportunities to download. JSON string input required')
    .demand(['h', 't'])
    .argv;

var restApi = h.getAPI(input),
    oppCollection = h.getCollection(restApi, 'app.opportunity'),
    quotesCollection = h.getCollection(restApi, 'app.quote'),
    offersCollection = h.getCollection(restApi, 'app.offer'),
    csvHelper = new csvHelperInstance();

// Select the quote if exists in the order of Primary, latest, quote  and base 
var scanQuotes = function (opp, callback) {


      var quoteIds = (!_.isEmpty(h.getRelKeys(opp, 'primaryQuote')) ? h.getRelKeys(opp, 'primaryQuote') :
                    (!_.isEmpty(h.getRelKeys(opp, 'latestQuote')) ? h.getRelKeys(opp, 'latestQuote')   :
                    (!_.isEmpty(h.getRelKeys(opp, 'quote')) ?  h.getRelKeys(opp, 'quote') :
                    h.getRelKeys(opp, 'baseQuote') ) ) );


    if (_.isEmpty(quoteIds)) return callback(null, []);
    else quoteIds = [quoteIds];

    quotesCollection.find({
        _id: {
            $in: quoteIds
        }
    }, {}, function (err, records) {
        return callback(err, records || []);
    });
};

// Select all offers associated with the quote
var scanOffers = function (quotes, opp, callback) {
    
    if (_.isEmpty(quotes)) return callback(null, []);

    var qs = _.pluck(quotes, '_id');
    
    offersCollection.find({
        'relationships.quote.targets.key': {
            $in: qs
        }
    }, {}, function (err, records) {
        return callback(err, records || []);
    });
};

// Updating the sum of all offers into opportunity 
var updateOpps = function (opp, listPrice, yearListPrice,  cb) {
    if (!opp.extensions.master.opportunityListPrice) {
        opp.extensions.master.opportunityListPrice = {
            type: 'core.currency',
            value: {
                amount: listPrice,
                code: {
                    name: ''
                }
            }
        };
    }
    if (!opp.extensions.tenant) {
        opp.extensions.tenant = {
             opportunityThisYearListPrice: {
             				value: {
                				amount: yearListPrice
               				}
		}
        };
    }
    if (!opp.extensions.tenant.opportunityThisYearListPrice) {
        opp.extensions.tenant.opportunityThisYearListPrice = {
             value: {
                amount: yearListPrice
               }
        };
    }
    if (!opp.extensions.tenant.opportunityThisYearListPrice.value) {
        opp.extensions.tenant.opportunityThisYearListPrice.value = {
                amount: yearListPrice
        };
    }
 console.log("Opp1 => ", opp.extensions.master.opportunityListPrice.value.amount, " And ", opp.extensions.tenant.opportunityThisYearListPrice.value.amount);
 console.log("Off => ", listPrice, " And ", yearListPrice);

    	opp.extensions.master.opportunityListPrice.value.amount = listPrice;
    	opp.extensions.tenant.opportunityThisYearListPrice.value.amount = yearListPrice;
    	oppCollection.update(opp, cb);

 console.log("Opp2 => ", opp.extensions.master.opportunityListPrice.value.amount, " And ", opp.extensions.tenant.opportunityThisYearListPrice.value.amount);
}

var processOpps = function (opp, callback) {
    var done = function (err) {
        if (err)
            log('error', "Skipping  opportunity '" + opp._id + "': " + JSON.stringify(err));
        else
            log('info', "Skipping  opportunity '" + opp._id + "'");

        return callback();
    };

    var listPrice = 0;
    var yearListPrice = 0;
    var currName = '';

    console.log("OppID =>", opp._id);
    scanQuotes(opp, function (err, quotes) {
        if (err) return done(err);

        scanOffers(quotes, opp, function (err, offers) {
            if (err) return done(err);
            _.each(offers, function (r) {


                if (!r.extensions.tenant.listPrice.value.amount) {
                        listPrice += 0; 
                }
                else  {  
                        listPrice += Number(r.extensions.tenant.listPrice.value.amount);}
    
               if (!r.extensions.tenant.opportunityThisYearListPrice || !r.extensions.tenant.opportunityThisYearListPrice.value) {
                     yearListPrice  += 0;
                }
                else { yearListPrice  += r.extensions.tenant.opportunityThisYearListPrice.value.amount; }


            });
            // callback;
            	updateOpps(opp, listPrice, yearListPrice,  callback);
        });
    });
}

var findOpps = function (callback) {
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
        multiple : true,
        limit : 5000,
        stream: false
    }, callback) ;

};
findOpps(function (err, opps) {
    if (err) {
        return console.log('Done ' + err);
    }
    async.eachLimit(opps, 5, processOpps, function (err, res) {
        console.log('Done processing all records ' + err);
    });
});
