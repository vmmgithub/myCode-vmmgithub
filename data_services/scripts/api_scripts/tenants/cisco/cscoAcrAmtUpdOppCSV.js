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
    .alias('l', 'limit').describe('l', 'Concurrent threads').default ('l', 7)
    .alias('d', 'displayName').describe('d', 'Display Name of the opportunity to download')
    .alias('g', 'tags').describe('g', 'Tag associated with the opportunity to download')
//    .alias('b', 'searchBy').describe('b', 'Generic filter associated with the opportunities to download. JSON string input required')
    .demand(['h', 't'])
    .argv;

var restApi = h.getAPI(input),
    oppCollection = h.getCollection(restApi, 'app.opportunity'),
    quotesCollection = h.getCollection(restApi, 'app.quote'),
    offersCollection = h.getCollection(restApi, 'app.offer'),
    csvHelper = new csvHelperInstance();

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
var updateOpps = function (opp, acrAmount, cb) {
    if (!opp.extensions.tenant) { opp.extensions.tenant={}; }
    if (!opp.extensions.tenant.acramount) { opp.extensions.tenant.acramount = {}; }
    if (!opp.extensions.tenant.acramount.value)
        { opp.extensions.tenant.acramount.value = {};}

    if (!opp.extensions.tenant.acramount.value.amount)
        { opp.extensions.tenant.acramount.value.amount = 0;  }

    if (_.isString(opp.extensions.tenant.acramount.value)) {
           console.log("string");
           opp.extensions.tenant.acramount.value={};
           opp.extensions.tenant.acramount.value.amount = 0;
    }

    	oppCollection.update(opp, cb);
}

var processOpps = function (searchBy, searchByValue, callback) {
    var done = function (err) {
        if (err)
            log('error', "Skipping  opportunity '" + searchByValue + "': " + JSON.stringify(err));
        else
            log('info', "Skipping  opportunity '" + searchByValue + "'");
        return callback();
    };

    findSource(searchBy.fieldName, searchByValue, function(err, res) {
        if (err) return done(err);

       async.eachLimit(res, input.limit, function(opp, cb) {
            var acrAmount = 0;

/*
            console.log("OppID =>", opp._id);
            scanQuotes(opp, function (err, quotes) {
                if (err) return done(err);
        
                scanOffers(quotes, opp, function (err, offers) {
                    if (err) return done(err);
                    _.each(offers, function (r) {
                         console.log("OfferID==>", r._id); 

                        if (!r.extensions.tenant.acramount) {
                                acrAmount += 0; 
                        }
                        else  {console.log(" Else listPrice==>", r.extensions.tenant.acramount);	
                               if (r.extensions.tenant.acramount.amount) {
                                acrAmount += Number(r.extensions.tenant.acramount.amount);
                                 }
                                 console.log("ACR 1=> ", acrAmount);
                                 }
            
        
                    });
        //            callback;
                    console.log("ACR=> ", acrAmount);
                    updateOpps(opp, acrAmount, callback);
                });
            });
*/
        updateOpps(opp, acrAmount, callback);
       }, done);
    });
}
//  Identify the column supplied in the csv file and return datatype and field name of the column
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

csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, 5, function (csvRecord, callback) {
        if (!data) return callback();
            if (!allColumnNames) {
                allColumnNames = _.keys(csvRecord);     // contains column names and datatype
                globalSearchBy = getDataType(_.first(allColumnNames));    //  Identify searchBy(first column) from all
            }
            var searchByValue = _.first(_.values(csvRecord));
            if (globalSearchBy.fieldName == '_id' || globalSearchBy.fieldName == 'externalIds.id' || globalSearchBy.fieldName == 'displayName' )
                processOpps(globalSearchBy, searchByValue, callback);
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
