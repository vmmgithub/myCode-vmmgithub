#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility  creates quote , using a CSV input.\
        \nThe input file should contain two columns, "Name" with the opportunity displayName and "Reason" code.\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', '_id')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    quoteCollection = h.getCollection(restApi, "app.quotes"),
    csvHelper = new csvHelperInstance();

var findOpportunity = function (value, callback) {
    var filter = {};
    filter[input.searchBy] = value;
    filter['flows.salesStages.state.name'] = "quoteCompleted";

    h.findRecords(oppCollection, {
        multiple: input.multiple,
        filter: filter,
    }, callback);
};

var findQuote = function (value, callback) {
    var filter = {};
    filter[input.searchBy] = value;
    filter['flows.quoteStages.state.name'] = "completed";

    h.findRecords(quoteCollection, {
        multiple: input.multiple,
        filter: filter,
    }, callback);
};

var sendQuote = function (opportunity, quote, callback) {
    var payload = {
        detail: {
            _id: quote._id,
            displayName: quote.displayName,
            type: quote.type
        }
    };

    tenantApi.execute('app.quote', opportunity._id, 'getSendQuoteInput', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['app.send.quote.input'] || !res.data['app.send.quote.input'][0])
            return callback("on getSendQuoteInput " + JSON.stringify(err || res));

        tenantApi.execute('app.quote', opportunity._id, 'sendQuote', res.data['app.send.quote.input'][0], function(err, res) {
            return callback(err, res.data);
        });
    });
};

var processRecord = function (oppName, quoteName, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "Delivering opportunity '" + oppName + "': " + JSON.stringify(err));
        } else {
            h.log('info', "Delivering opportunity '" + oppName );
        }
        callback();
    };

    findOpportunity(oppName, function(err, opps) {
        if (err) return done(err);

        findQuote(quoteName, function(err, quotes) {
            if (err) return done(err);

            sendQuote(opps[0], quotes[0], done); 
        });
    });
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();
            var oppName = csvRecord["Name"];
            var quoteName = csvRecord["Quote Name"];

            if (oppName && quoteName) {
                processRecord(oppName, quoteName, callback);
            } else {
                h.log('warn', 'Skipping ' + oppName );
                callback();
            }
        },
        function (err) {
            h.log('info', 'DONE ' + err);
        });
});