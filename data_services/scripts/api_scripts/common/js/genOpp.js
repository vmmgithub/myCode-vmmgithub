#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility generates opportunities.\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'filter').describe('f', 'String version of JSON filter').default('f', '{}')    
    .alias('c', 'criteria').describe('c', 'String version of opportunity generation criteria').default('c', '{}')
    .alias('o', 'operation').describe('o', 'Operations [scanOnly|generate|generateAndPoll]').default('o', 'scanOnly')
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
    opportunityCollection = h.getCollection(restApi, 'app.opportunities'),
    assetCollection = h.getCollection(restApi, 'app.assets'),
    actionCollection = h.getCollection(restApi, 'core.actions'),
    globalJob,
    objCriteria={},
    globalTag;

var COLS = {
    assets: [
        '_id',
        'displayName',
        'externalIds.id',
        'startDate',
        'endDate',
        'amount',
        /*'extensions.master.batchType.value.name', 
        'extensions.master.batchQuarter.value', 
        'extensions.master.clientBatchQuarter.value',
        'extensions.master.clientTheatre.value.name', 
        'extensions.master.clientRegion.value.name', 
        'extensions.master.clientTerritory.value.name', 
        'extensions.master.country.value.name', */
        'relationships.customer.targets.key', 
        'relationships.product.targets.key', 
        //'relationships.reseller.targets.key', 
        //'relationships.distributor.targets.key', 
        //'relationships.covered.targets.key', 
    ],
    opportunities: [
        '_id',
        'displayName',
        'amount',
        'targetAmount',
        'targetDate',
    ],
    offers: [
        '_id',
        'amount',
        'targetAmount',
        'startDate',
        'endDate',
    ],
    quotes: [
    ] 
};

// Prep the filter
var init = function(callback) {
    try {
        if (_.isString(input.filter)) input.filter = JSON.parse(input.filter);

        // Can only generate opportunities for service assets without opps
        input.filter.associatedOpportunity = false;
        input.filter.type = 'app.asset/service';
        input.filter.qRank = '4';


        if (_.isString(input.criteria)) {
               h.strToObj(objCriteria, input.criteria);
        }

        callback();
    } catch (err) {
        callback(err);
    }
};


//TODO complete this scan to be better
var scanAssetsForReqFields = function(asset) {
    _.each(COLS.assets, function(path) {
        var r = h.getObjectValueFromPath(asset, path);
        if (!r) h.log('warn', asset._id + ' missing ' + path + ' ' + r);
    });
};

//TODO complete this scan to be better
var scanOppsForReqFields = function(opp) {
    _.each(COLS.opportunities, function(path) {
        if (h.contains(opp.displayName, 'NotDetermined')) h.log('warn', opp._id + ' incomplete with displayName = ' + displayName);
    });
};

var findAssets = function (callback) {
    h.findRecords(assetCollection, {
        multiple: true,
        filter: input.filter,
        limit: 20000,
        columns: COLS.assets,
    }, function(err, assets) {
        _.each(assets, scanAssetsForReqFields);
        return callback();      
    });
};

var startOppGen = function(callback) {
    var payload = {
        detail: null,
        selectAll: {
            filter: input.filter, //check for JSON/string
            avalonCollectionName: 'app.assets'
        },
        configParams: {
            scriptName: 'genOffer', //change by tenant?
            inputModel: 'app.genopp.defaultinput',
            projection: ['_id'],
            scriptConfig: { //change by tenant?
                batchType: 'renewal' 
            },
            refreshParentOnSave: false
        }
    };

    tenantApi.execute('app.assets', null, 'getGenOppUsingFindAndProcessInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.genopp.defaultinput'] || !res.data['app.genopp.defaultinput'][0]) 
            return callback("on gen input " + JSON.stringify(err || res));

        var genPayload = res.data['app.genopp.defaultinput'][0];
        _.extend(genPayload, objCriteria);

        tenantApi.execute('app.assets', null, 'genOppUsingFindAndProcess', genPayload, function(err, res) {
            if (err || !res || !res.success || !res.messages || !res.messages[0] || !res.messages[0].bgJob) 
                return callback("on gen kickoff " + JSON.stringify(err || res));

            var job = res.messages[0].bgJob;
            var tag = '';
            if (job.name && h.contains(job.name, ':')) tag = job.name.split(':')[2];

            callback(err, tag, job);
        });

    });

};

var pollJobCompletion = function(job, callback) {
    async.until(
        function() {
            return job.status && job.status.name == 'completed';
        }, 
        function(cb) {
            _.delay(function() {
                h.findRecords(actionCollection, {
                    filter: {_id: job._id},
                }, function(err, res) {
                    if (err || !res) return cb(err || 'No job information');
                    
                    job = res && res[0];
                    h.log('debug', 'Job status is ' + job.status.name);
                    cb();
                });                
            }, 1 * 10 * 1000);
        }, 
        callback);
};

var scanGeneratedOpportunities = function(tag, callback) {
    h.findRecords(opportunityCollection, {
        multiple: true,
        filter: {tags: tag},
        limit: 20000,
        columns: COLS.opportunities,
    }, function(err, opportunities) {
        _.each(opportunities, scanAssetsForReqFields);
        return callback();      
    });
};

async.waterfall([
    function(cb) {
        init(function(err) {
            if (err) return cb('Problem with input filter or criteria ' + err);
            h.log('info', 'Initiating ... ' + input.operation);
            h.log('debug', '    Filter is ... ' + JSON.stringify(input.filter));
            h.log('debug', '    Criter is ... ' + JSON.stringify(input.criteria));

            return cb();
        });
    },
    function(cb) {
        if (input.operation != 'scanOnly') return cb(); //skipping the work

        findAssets(function(err) {
            if (err) return cb('Problem with scanning assets ' + err);
            h.log('debug', 'Completed scan');
            return cb();
        });
    },
    function(cb) {
        if (input.operation == 'scanOnly') return cb(); //skipping the work

        h.log('debug', 'Initiating opp gen ');
        startOppGen(function(err, tag, job) {
            if (err) return cb('Problem with opp gen ' + err);
            globalJob = job;
            globalTag = tag;

            h.log('info', 'Opp gen started with ' + globalTag + ' and ' + globalJob._id);
            return cb(err);
        });
    },
    function(cb) {
        if (input.operation != 'generateAndPoll') return cb(); //skipping the work

        pollJobCompletion(globalJob, function(err, job) {
            if (err) return cb('Problem checking status ' + err);

            return cb(err);
        });
    },
    function(cb) {
        if (input.operation != 'generateAndPoll') return cb(); //skipping the work

        scanGeneratedOpportunities(globalTag, function(err, job) {
            if (err) return cb('Problem checking opp status ' + err);

            return cb(err, job);
        });
    },    
], 
function(err, res) {
    if (err) 
        h.log('error', err);
    else 
        h.log('info', 'Done');
});
