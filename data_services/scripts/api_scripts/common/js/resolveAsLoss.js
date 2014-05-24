#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility resolves opportunity as a loss in Renew, using a CSV input.\
        \nThe input file should contain two columns, "Name" with the opportunity displayName and "Reason" code.\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', 'displayName')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    lookupCollection = h.getCollection(restApi, "app.lookups"),
    csvHelper = new csvHelperInstance();

var findOpportunity = function (value, callback) {
    h.findRecords(oppCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var findReason = function (value, callback) {
    h.findCachedRecords(lookupCollection, {
        filter: {displayName: value},
    }, callback);
};

var getResolveAsLossInput = function (opportunity, callback) {
    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        }
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'getResolveAsLossInput', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['app.resolve.loss.input'] || !res.data['app.resolve.loss.input'][0]) 
            return callback("on loss input " + JSON.stringify(err));

        return callback(null, res.data['app.resolve.loss.input'][0]);

    });
};

var resolveAsLossAction = function (opportunity, input, lossDate, lossReason, callback) {
    input.resultReason = {
        name: lossReason.name,
        displayName: lossReason.displayName
    };

    if (lossDate) input.lossDate = lossDate;
    input.notes = [{text: 'Automated resolve as loss', type: 'core.note'}];

    tenantApi.execute('app.opportunities', opportunity._id, 'resolveAsLoss', input, function(err, res) {
        if (err || res.success == false) 
            return callback(" setting loss input " + JSON.stringify(err || res));

        return callback(null, res && res.data);

    });
};

var processRecord = function (oppName, lossDate, lossReason, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "Resolving opportunity '" + oppName + "': " + JSON.stringify(err));
        } else {
            h.log('info', "Resolving opportunity '" + oppName + "' with " + lossReason);
        }

        callback();
    };

    findReason(lossReason, function(err, reason) {
        if (err) return done(err);

        findOpportunity(oppName, function(err, res) {
            if (err) return done(err);

            async.eachLimit(res, 1, function(opportunity, ocb) {
                getResolveAsLossInput(opportunity, function(err, input) {
                    if (err) return done(err);
                    resolveAsLossAction(opportunity, input, lossDate, reason, ocb);
                });
            },
            done);
        });
    });
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();
            var oppName = csvRecord["Name"];
            var lossReason = csvRecord["Reason"];
            var lossDate = csvRecord["LossDate"];

            if (oppName && lossReason) {
                processRecord(oppName, lossDate, lossReason, callback);
            } else {
                h.log('warn', 'Skipping ' + oppName + ' and ' + lossReason);
                callback();
            }
        },
        function (err) {
            h.log('info', 'DONE ' + err);
        });
});
