#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");
var fs = require('fs');

var input = require('optimist')
    .usage('\nREADME: This is a utility used for scrubbing opportunities in Renew, based on Atlas values.\
        \n\nThe input file should contain five columns: ...... where: \
        \n Opportunity Id has the externalId, of opportunity that needs to be updated\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', true)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('k', 'skip').describe('k', 'Number of lines to skip in the input file').default('k', 0)
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalIds.id]').default('b', 'extensions.tenant.atlasMigrationId.value')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    csvHelper = new csvHelperInstance();

var findOpportunity = function (value, callback) {
    h.findRecords(oppCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var updateFlowTran = function(opp, toState, changeDate, modified) {
    if (!changeDate || !toState) return;
    if (_.isString(changeDate)) changeDate = moment(changeDate);

    _.each(opp.flows.salesStages.transitions, function(t, i) {
        if (t.toState == toState) {
            modified = true;

            t.changeDate = changeDate.toISOString();
            if (i != 0) {
                var prev = moment(opp.flows.salesStages.transitions[i-1].changeDate);
                t.timeGap = prev.diff(changeDate);
            }
        }
    });
}

var doTheWork = function (csvRecord, rowNum, callback) {
    if (!csvRecord) return callback();

    var oppId = csvRecord['opportunityid'];
    var salesStage = csvRecord['ssisalesstage'];
    var commitLevel = csvRecord['commitlevel'];
    var resolutionDate = csvRecord['resolutiondate'] && moment(csvRecord['resolutiondate']);
    var contactedDate = csvRecord['firstcontactdate'] && moment(csvRecord['firstcontactdate']);
    var quotedDate = csvRecord['firstquotedate'] && moment(csvRecord['firstquotedate']);
    var poDate = csvRecord['ssibookingdate'] && moment(csvRecord['ssibookingdate']);
    var soDate = csvRecord['clientbookingdate'] && moment(csvRecord['clientbookingdate']);
    var poNumber = csvRecord['newponumber'];
    var poAmount = csvRecord['localtransactionamount'];
    var soNumber = csvRecord['newsonumber'];
    var resultReason = csvRecord['ssiresultreason'];
    var amount = csvRecord['localtransactionamount'];
    var quoteId = csvRecord['ssiquotenumberid'];

    if (resultReason == '<none>') resultReason = null;
    if (salesStage == 'Quote Request') salesStage = 'Quote Requested';
    if (contactedDate && resolutionDate && contactedDate.isAfter(resolutionDate)) contactedDate = resolutionDate;
    if (quotedDate && resolutionDate && quotedDate.isAfter(resolutionDate)) quotedDate = resolutionDate;

    if (!oppId) {
        h.log('info', 'Skipping ' + csvRecord.toString() + ' because of missing data');
        return callback();
    }

    findOpportunity(oppId, function(err, res) {
        if (err || !res || res.length == 0) return callback();

        var innerDone = _.after(res.length, function(err, res) {
            if (err)
                h.log('error', '' + rowNum + ' Unable to set flow dates ' + oppId + ' ' + err);
            else                
                h.log('info', '' + rowNum + ' Done setting flow dates ' + oppId);
            callback();
        });

        _.each(res, function(opp) {
            var modified = false;

            if (contactedDate) {
                updateFlowTran(opp, 'notContacted', contactedDate, modified);
                updateFlowTran(opp, 'contacted', contactedDate, modified);
            }
            
            if (quotedDate) {
                updateFlowTran(opp, 'quoteRequested', quotedDate, modified);
                updateFlowTran(opp, 'quoteCompleted', quotedDate, modified);
                updateFlowTran(opp, 'quoteDelivered', quotedDate, modified);
            }

            if (resolutionDate) {
                updateFlowTran(opp, 'customerCommitment', resolutionDate, modified);
                updateFlowTran(opp, 'poReceived', resolutionDate, modified);
                updateFlowTran(opp, 'closedSale', resolutionDate, modified);
                updateFlowTran(opp, 'houseAccount', resolutionDate, modified);
                updateFlowTran(opp, 'noService', resolutionDate, modified);
            }

            if (resolutionDate && h.isoDate(resolutionDate) != h.isoDate(opp.resolutionDate)) {
                opp.resolutionDate = resolutionDate.toISOString();
                modified = true;
            }

            if (modified) {
                h.log('debug', 'Updating opp ' + oppId + ' ' + opp.displayName);
                oppCollection.update(opp, innerDone);
            } else {
                h.log('debug', 'Skipping opp ' + oppId + ' ' + opp.displayName);
                innerDone();
            }
        });
    });
}

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    data = _.rest(data, input.skip);
    var rowNum = input.skip;   

    async.eachLimit(data, input.limit, 
        function(csv, cb) {
            doTheWork(csv, ++rowNum, cb);
        },
        function (err) {
            h.log('info', 'DONE ' + err);
        }
    );
});
