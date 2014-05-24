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
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 1)
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
    
//    var quoteIds = (h.getRelKey(opp, 'primaryQuote') || h.getRelKeys(opp, 'latestQuote') || h.getRelKeys(opp, 'baseQuote'));
      var quoteIds = (!_.isEmpty(h.getRelKeys(opp, 'primaryQuote')) ? h.getRelKeys(opp, 'primaryQuote') :
                    (!_.isEmpty(h.getRelKeys(opp, 'latestQuote')) ? h.getRelKeys(opp, 'latestQuote')   :
                    (!_.isEmpty(h.getRelKeys(opp, 'quote')) ?  h.getRelKeys(opp, 'quote') :
                    h.getRelKeys(opp, 'baseQuote') ) ) );

    if (_.isEmpty(quoteIds)) return callback(null, []);
    quotesCollection.find({_id: {$in: quoteIds}}, {}, function(err, records) {
        return callback(err, records || []);
    });
};

// Select all offers associated with the quote
var scanOffers = function (quotes, opp,  callback) {
    if (_.isEmpty(quotes)) return callback(null, []);

    var qs = _.pluck(quotes, '_id');

    offersCollection.find({'relationships.quote.targets.key': {$in: qs}}, {}, function(err, records) {
        return callback(err, records || []);
    });
};

// Updating the sum of all offers into opportunity 
var updateOpps = function (opp, listPrice, notAnnualizedAmt, currName, cb) {
     if (!opp.extensions.tenant.listPrice) {
         opp.extensions.tenant.listPrice = {type: 'core.currency', value: {amount: listPrice, code: { name: currName}}};
     }

     if (!opp.extensions.tenant.priorRenewalAmountNotAnnualized) {
         opp.extensions.tenant.priorRenewalAmountNotAnnualized = {type: 'core.currency', value: {amount: notAnnualizedAmt, code: { name : currName}}};
     }
    if (opp.extensions.tenant.listPrice.amount && opp.extensions.tenant.priorRenewalAmountNotAnnualized.value ) {
     console.log("Opp1 => ", opp.extensions.tenant.listPrice.amount, " And ",opp.extensions.tenant.priorRenewalAmountNotAnnualized.value.amount);
     }
     console.log("Off => ", listPrice, " And ",notAnnualizedAmt);
//     if ((opp.extensions.tenant.listPrice.amount !== listPrice) || (opp.extensions.tenant.priorRenewalAmountNotAnnualized.value.amount !== notAnnualizedAmt)) {
       
     	opp.extensions.tenant.listPrice.value.amount = listPrice;
     	opp.extensions.tenant.priorRenewalAmountNotAnnualized.value.amount = notAnnualizedAmt;
     	oppCollection.update(opp, cb);
//     }
     console.log("Opp => ", opp.extensions.tenant.listPrice.amount, " And ",opp.extensions.tenant.priorRenewalAmountNotAnnualized.value.amount);
}

var processOpps = function (opp, callback) {
    var done = function(err) {
        if (err)
            log('error', "Skipping  opportunity '" + searchByValue + "': " + JSON.stringify(err));
        else
            log('info', "Skipping  opportunity '" + searchByValue + "'");

        return callback();
    };

     var listPrice=0;
     var notAnnualizedAmt=0;
     var currName='' ;

      console.log("OppID =>", opp._id, " ", opp.flows.salesStages.state.name);
      scanQuotes(opp, function(err, quotes) {
         if (err)  return done(err);

          scanOffers (quotes, opp,  function(err, offers) {
               if (err)  return done(err);
              _.each(offers,function(r) {   
                                          if (r.extensions.tenant.listprice) { 
			                  listPrice += r.extensions.tenant.listPrice.amount ; }
                                          if (r.extensions.tenant.priorRenewalAmountNotAnnualized) {
			                      notAnnualizedAmt += r.extensions.tenant.priorRenewalAmountNotAnnualized.value.amount ;
                                              currName = r.extensions.tenant.listPrice.code.name; 
                                           }
              });
               updateOpps(opp, listPrice, notAnnualizedAmt, currName, callback);
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
        limit : 35000,
        stream: false
    }, callback) ;
    
};

findOpps(function(err, opps) {
      if (err) {return console.log('Done ' + err);}
     async.eachLimit(opps, input.limit, processOpps, function(err, res) {
          console.log('Done processing all records ' + err);
      });
});
