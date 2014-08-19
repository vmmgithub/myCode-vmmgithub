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
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', '_id')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('z', 'zenMode').describe('z', 'Zen mode handles most exceptions and forces opp close').default('z', false)
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
        filter: {
            "$or": [{name: value}, {displayName: value}]
        },
        value: value
    }, callback);
};

var adamantFunction = function(opportunity, params, lossDate, lossReason, callback) {
    if (h.getFlowState(opportunity, 'salesStages') == 'houseAccount' || h.getFlowState(opportunity, 'salesStages') == 'noService') {
        h.log('debug', 'Opportunity already lost ' + opportunity._id);
        return callback();
    }

    var determineNextSteps = function(err, res) {
        //Get out if not in zen mode or if we max out on 2 retries
        return callback(err, res);
    };

    resolveAsLossAction(opportunity, params, lossDate, lossReason, determineNextSteps);
}

var resolveAsLossAction = function (opportunity, params, lossDate, lossReason, callback) {
    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        }
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'getResolveAsLossInput', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['app.resolve.loss.input'] || !res.data['app.resolve.loss.input'][0]) 
            return callback(err || res);

        var input = res.data['app.resolve.loss.input'][0];

        input.resultReason = {
            name: lossReason.name,
            displayName: lossReason.displayName
        };

        if (lossDate) input.lossDate = h.noonOffset(lossDate);
        input.notes = [{text: 'Automated resolve as loss', type: 'core.note'}];

        tenantApi.execute('app.opportunities', opportunity._id, 'resolveAsLoss', input, function(err, res) {
            if (err || res.success == false) 
                return callback(err || res);

            return callback(null, res && res.data);
        });

    });
};

var updateSellingPeriod = function (opp, resolutionDate, callback) {
    var r = h.getTargetSellingPeriod(resolutionDate);
    if (!r || (opp.extensions.master.targetPeriod && opp.extensions.master.targetPeriod.value 
            && opp.extensions.master.targetPeriod.value.name == r.name)) {
        return callback(null, opp);
    }

    var opps = [opp];
    h.getMasterOpp(oppCollection, tenantApi, opp, function(err, mopp) {
        if (mopp._id != opp._id) opps.push(mopp);

        async.each(opps, function(op, cb) {
            var o = {
                _id: op._id,
                extensions: {
                    master: {
                        targetPeriod: op.extensions.master.targetPeriod
                    }
                },
                systemProperties: op.systemProperties
            };
            if (!o.extensions.master.targetPeriod) 
                o.extensions.master.targetPeriod = {};

            o.extensions.master.targetPeriod.value = r;
            h.log('debug', "Changing selling period on '" + o._id + "' to " + r.name);

            oppCollection.update(o, cb);
        }, callback); 
    });
};

var processRecord = function (oppName, lossDate, lossReason, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "Resolving opportunity '" + oppName + "': " + JSON.stringify(err));
            h.print('FAIL|', [oppName, lossReason, lossDate]);
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
                updateSellingPeriod(opportunity, lossDate, function(err, opp) {
                    if (err) return ocb(err);
                    adamantFunction(opportunity, {tries: 0}, lossDate, reason, ocb);
                });                
            },
            done);
        });
    });
};

h.log('info', 'Processing ' + input.file);
h.print('FAIL|', ['Name', 'Reason', 'LossDate']); // for auto re-processing

// Read the selling periods and update it prior to proceeding with other steps
h.initLookups(restApi, 'app.opportunity', function(err) {
    csvHelper.readAsObj(input.file, function (data) {
        if (!data) return callback();

        async.eachLimit(data, input.limit, function (csvRecord, callback) {
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
});
