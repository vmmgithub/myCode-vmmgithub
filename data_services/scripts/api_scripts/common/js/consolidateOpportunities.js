#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility that consolidates opportunities in Renew, using a CSV input.\
        \nThe input file should contain two columns, "targetOppId" and "sourceOppIds".\
        \ntargetOppId is the opportunity _id that will get consolidated and have a Sales Stage of "Consolidated".\
        \nsourceOppIds is the opportunity _id that the targetOppId is consolidated into. This inherits\
        \nall of the offers from the targetOppId opportunity.\
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
    csvHelper = new csvHelperInstance();

var processRecord = function (targetOppId, sourceOppIds, callback) {
    var done = function(err) {
        if (err) {
           h.log('error', "Consolidating opportunity '" + targetOppId + "': " + JSON.stringify(err));
        } else {
            h.log('info', "Consolidating opportunity '" + targetOppId + "' with " + (sourceOppIds));
        }
        callback();
    };

    var payload = {
        selections: []
    };

    payload.selections.push({_id: targetOppId, type: 'app.opportunity'});
    _.each(sourceOppIds, function(sOppId) {
        payload.selections.push({_id: sOppId, type: 'app.opportunity'});
    });

    tenantApi.execute('app.opportunities', null, 'getCombineOpportunitiesInput', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['app.combine.opportunities.input'] || !res.data['app.combine.opportunities.input'][0])
            return done("on consolidate input " + JSON.stringify(err || res));

        tenantApi.execute('app.opportunities', null, 'combineOpportunities', res.data['app.combine.opportunities.input'][0], function(err, res) {
            if (err || res.success == false)
                return done(" consolidate " + JSON.stringify(err || res));

            return done(null, res.data['app.opportunities']);
        });
    });
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();
            var targetOppId = csvRecord["targetOppId"];
            var sourceOppIds = csvRecord["sourceOppIds"];
            sourceOppIds = sourceOppIds.split('|');

            if (targetOppId && sourceOppIds && sourceOppIds.length > 0) {
                processRecord(targetOppId, sourceOppIds, callback);
            } else {
                h.log('Skipping ' + targetOppId);
                callback();
            }
        },
        function (err) {
            h.log('info', 'DONE ' + err);
        });
});
