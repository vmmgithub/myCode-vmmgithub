#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility deletes opportunities.\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 3)
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', '_id')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('o', 'operation').describe('o', 'Operations [scanOnly|clearOpportunities]').default('o', 'scanOnly')
    .demand(['h', 't'])
    .argv;

/* 
v1: Opp Gen to get kicked off with a filter & criteria (JSON), monitor completion of the background job, scan every X minutes & complete when the job finishes
v2: Scan assets for required fields to be present + v1
v3: scan of generated opps/offers for validity (all opps should have offers, "Not Determined" should not be in the name, expirationDate cannot be 9999 ....)
v4: streaming find of assets + configurable asset column check + v2
*/

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    csvHelper = new csvHelperInstance();

var findOpportunity = function (value, callback) {
    h.findRecords(oppCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        columns: ['displayName', '_id', 'type', 'systemProperties'],
        value: value,
    }, callback);
};

var clearOpps = function(opp, callback) {
    var payload = {
      detail: null,
      selections: [ opp ],
      selectAll: null,
      configParams: {
        action: "clearOpportunities",
        type: "app.opportunity",
        "static": true,
        skipDetail: true,
        refreshParentOnSave: false
      }
    };

    tenantApi.execute('app.opportunities', null, 'clearOpportunities', payload, function(err, res) {
        if (err || !res || !res.success) 
            return callback("on clearOpportunities " + JSON.stringify(err || res));
        return callback(null, true);
    });
};

var processRecord = function (oppName, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "Clearing opportunity '" + oppName + "': " + JSON.stringify(err));
            h.print('FAIL|', [oppName]);
        }
        callback();
    };

    findOpportunity(oppName, function(err, opps) {
        if (err) return done(err);

        async.eachLimit(opps, 1, function(opp, cb) {
            if (input.operation == 'clearOpportunities') clearOpps(opp, cb);
            else {h.log('debug', 'skipping based on operation mode ' + input.operation); return cb();}
        }, done);
    });
};

h.log('info', 'Processing ' + input.file);
h.print('FAIL|', ['Opportunity']); // for auto re-processing

csvHelper.readAsObj(input.file, function (data) {
    if (!data) return callback();
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        var oppName = csvRecord["Opportunity"];

        if (oppName) {
            processRecord(oppName, callback);
        } else {
            h.log('warn', 'Skipping ' + oppName);
            callback();
        }
    },
    function (err) {
        h.log('info', "DONE " + err);
    });
});
