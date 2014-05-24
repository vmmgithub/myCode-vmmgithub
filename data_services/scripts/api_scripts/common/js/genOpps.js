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
    .demand(['h', 't', 'f'])
    .argv;

/* 
v1: Opp Gen to get kicked off with a filter & criteria (JSON), monitor completion of the background job, scan every X minutes & complete when the job finishes
v2: Scan assets for required fields to be present + v1
v3: scan of generated opps/offers for validity (all opps should have offers, "Not Determined" should not be in the name, expirationDate cannot be 9999 ....)
v4: streaming find of assets + configurable asset column check + v2
*/

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    assetCollection = h.getCollection(restApi, 'app.assets'),
    actionCollection = h.getCollection(restApi, 'core.actions');

// Prep the filter
var init = function() {
    input.filter['associatedOpportunity'] = false;
    input.filter['type'] = 'app.asset/service';

    try {
        if (_.isString(input.criteria)) input.criteria = JSON.parse(input.criteria);
    } catch (err) {
        input.criteria = {};
    }
};

var findAssets = function (callback) {
    var cols = [
        'displayName',
        'externalIds.id',
        'startDate',
        'endDate',
        'extensions.master.clientBatchQuarter.value',
        'extensions.master.targetPeriod.value.name', // check if you need additional columns to look for mandatory fields
    ];

    h.findRecords(assetCollection, {
        multiple: true,
        filter: input.filter,
        limit: 200000,
        columns: cols,
        stream : false,
    }, callback);
};


var addExtensions = function(obj, criteria, callback) {
    if (!obj ) callback(null, obj);

    console.log("CRITERIA===>", criteria);


    if (!_.isObject(obj.extensions)) obj.extensions = {};
 //   obj.extensions.push(criteria);

    return (obj, callback);
};

var startOppGen = function(callback) {
    var payload = {
        'detail': null,
        'selectAll': {
            'filter': input.filter, //check for JSON/string
            'avalonCollectionName': 'app.assets'
        },
        'configParams': {
            'scriptName': 'genOffer',
            'inputModel': 'app.genopp.defaultinput',
            'projection': ['_id'],
            'scriptConfig': { 'batchType': 'renewal' },
            'refreshParentOnSave': false
        },
    };


    tenantApi.execute('app.assets', null, 'getGenOppUsingFindAndProcessInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.genopp.defaultinput'] || !res.data['app.genopp.defaultinput'][0]) 
            return callback("on gen input " + JSON.stringify(err || res));
        var r =  res.data['app.genopp.defaultinput'][0];
         

//        var c = JSON.parse(input.criteria);

//        var c = {};

        r.extensions={};
        r.extensions.master={};
        r.extensions.master.targetPeriod={};
        r.extensions.master.businessLine={};
        r.extensions.master.commitLevel={};
        r.extensions.master.targetPeriod.value={};
        r.extensions.master.businessLine.value={};
        r.extensions.master.commitLevel.value={};
        r.extensions.master.targetPeriod.value.name = 'fy15q1'
        r.extensions.master.businessLine.value.name = 'AV'
        r.extensions.master.commitLevel.value.name = 'black'

        console.log("final C criteria==>" , r);

/*
        var c = _.extend(c, r); 
        console.log("final input criteria==>" , c);
        console.log("final R ==>" , r);

*/
        tenantApi.execute('app.assets', null, 'genOppUsingFindAndProcess', r, function(err, res) {
            console.log("Res==>", res);
            if (err || !res || !res.success || !res.data || !res.data['bgJob'] || !res.data['bgJob'][0]) 
                return callback("on gen kickoff " + JSON.stringify(err || res));

            var r = res.data['bgJob'][0];
            var tag = '';

            if (r.name && h.contains(r.name, ':')) {
                tag = r.name.split(':')[2];
            }

        });

    });    
};

//todo to call startOppGen
startOppGen(function(err) {
    if (err) h.log('error',err);
});
