#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('../../common/js/helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility to add or remove attributes for any object in Renew, using a CSV input.\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('n', 'port').describe('n', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password')
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('o', 'operation').describe('o', 'Operation to perform [cruiseControl, listJobs, resetJobs, countJobs]').default('o', 'countJobs')
    .alias('m', 'match').describe('m', 'String to match in the name of the DL job')
    .alias('i', 'incomplete').describe('i', 'Flag to denote if we need to count incomplete DLs').default('i', false)
    .demand(['h', 't'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    dlCollection = h.getCollection(restApi, 'app.dataloads'),
	csvHelper = new csvHelperInstance();

// Utility functions
var findDLs = function (filter, params, callback) {
    if (!filter["status.name"] && !filter._id) {
        if (input.incomplete) 
            filter["status.name"] = {"$!regex":"^"};
        else
            filter["status.name"] = {$nin: ['completed', 'purged']};        
    }

    if (input.match) filter.displayName = {'$regex': input.match + '.*', '$options': 'i'};
    if (!params.limit) params.limit = 10000;

    h.findRecords(dlCollection, {
        multiple: true,
        filter: filter,
        params: params
    }, callback);
};

var getColSummary = function(dataload) {
    return dataload.inputSummary && dataload.inputSummary.collectionSummary && dataload.inputSummary.collectionSummary[0];
};

var getType = function(dataload) {
    var dn = (dataload.displayName || 'none');
    var type;

    if (dn.match(/person/i)) type = 'person';
    if (dn.match(/org/i)) type = 'org';
    if (dn.match(/affinity/i)) type = 'affinity';
    if (dn.match(/covered/i) && dn.match(/asset/i)) type = 'covered';
    if (dn.match(/service/i) && dn.match(/asset/i)) type = 'service';
    if (dn.match(/offer/i)) type = 'offer';
    if (dn.match(/opp/i)) type = 'opp';
    if (dn.match(/booking/i)) type = 'booking';
    if (dn.match(/quote/i)) type = 'quote';
    if (dn.match(/addr/i)) type = 'addr';
    if (dn.match(/customer/i)) type = 'customer';
    if (dn.match(/resolve/i)) type = 'resolve';
    if (dn.match(/lose/i)) type = 'lose';

    if (!type) type = (getColSummary(dataload) && getColSummary(dataload).collectionName) || 'none';
    return type;
};

var checkIfDone = function(id, cb) {
    findDLs({_id: id}, {limit: 1}, function(err, res) {
        if (err || !res) return cb(false);

        var x = res[0];
        var finished = false;
        var curr = 0;

        if (x.progress) {
            curr = _.reduce(_.pluck(_.values(x.progress), 'processed'), function(memo, num){if (num) return memo + num; else return memo; }, 0);
            var total = _.reduce(_.pluck(_.values(x.progress), 'total'), function(memo, num){ if (num) return memo + num; else return memo;}, 0);
            h.log('debug', 'Progress: ' + curr + ' out of ' + total + ' for ' + id + ' ' + x.displayName);
        }
        if (x.status.name == 'completed') finished = true;

        return cb(finished, curr);
    });
};

var doIt = function(filter, params, postBody, ocb) {
    var retryThreshold = input.retryThreshold || 21; // retry the load after 20 mins

    var linkFunc = function(dataload, postBody, retry) {
        tenantApi.execute('app.dataloads', dataload._id, 'postProcess', postBody, function(err, res) {
            h.log('info',' Post process started for ' + dataload._id + ' and ' + dataload.displayName);

            var lastProcessed = 0, retryCount = 0;
            var finished = false;
            var interval = setInterval(function () {
                checkIfDone(dataload._id, function(isDone, processed) {
                    finished = isDone;
                    if (finished) {
                        clearInterval(interval);
                        ocb(dataload);
                    } else if (retry) {
                        if (processed && processed !== lastProcessed) {
                            lastProcessed = processed;
                            retryCount = 0;
                        } else {
                            retryCount ++;
                            h.log('warn', 'Getting stuck for ' + dataload._id + ":" + dataload.displayName + " for " + retryCount + " time(s).");
                            if (retryCount === retryThreshold) {
                                clearInterval(interval);
                                h.log('info',"Retry the dataload again: " + dataload._id);
                                linkFunc(dataload);
                            }
                        }
                    }
                });
            },
            60000);
        });
    };

    findDLs(filter, params, function(err, res) {
        _.map(res, linkFunc);
    });
};

// Main functions 
var countJobs = function(callback) {
    findDLs({}, {}, function(err, res) {
        if (err) return callback(err);

        var total = {};
        _.each(res, function(dataload) {
            var s = (dataload.status && dataload.status.name) || 'none';
            var type = getType(dataload);

            if (!total[s]) total[s] = {};
            if (!total[s][type]) total[s][type] = {count: 0, type: type, jobs: 0, name: s, unknown: 0};

            var c = getColSummary(dataload) && getColSummary(dataload).numberRecords;
            if (c) total[s][type].count += c; else total[s][type].unknown ++;
            total[s][type].jobs ++;
        });

        _.each(total, function(s) {
            _.each(s, function(t) {
                h.log('info','Linking of ' + t.count + ' and ' + t.unknown + ' unknown [' + t.type + '] records across ' + t.jobs + ' jobs with '+ t.name + ' status');
            });
        });

        callback();
    });
};

var resetJobs = function(callback) {
    h.log('info', 'Started reset of data load jobs');
    findDLs({}, {}, function(err, res) {
        if (err) return callback(err);

        async.eachLimit(res, input.limit, function(dataload, ocb) {
            tenantApi.execute('app.dataloads', dataload._id, 'done', {defer: true}, function(err, res) {
                if (err) return ocb("on reseting jobs " + JSON.stringify(err));
                return ocb(null, res && res.data && res.data['app.dataload'] && res.data['app.dataload'][0]);
            });
        },
        callback);
    });
};

var listJobs = function(callback) {
    findDLs({}, {}, function(err, res) {
        if (err) return callback(err);

        _.each(res, function(dataload) {
            h.log('info', dataload._id + " " + dataload.displayName);
        });
        callback();
    });
};

var cruiseControl = function(callback) {
    //var payload = {parallelChops: 32, recordsPerChop: 250, vintage: false, retry:1, runOnce:1};
    var payload = {parallelChops: 32, recordsPerChop: 250, vintage: false, };
    var filter = {}; filter['status.name'] = 'pending';
    var params = {limit: 1, sort: {'systemProperties.createdOn': 1}};
    var cb = function (load) {
        if (load) {
            h.log('info',"Done postProcess for " + load._id + " " + load.displayName);
            doIt(filter, params, payload, cb);
        }
    }
    var count = 0, maxConc = input.limit || 2;
    var interval = setInterval ( function () {
        if (count >= maxConc) {
            clearInterval(interval);
            return;
        }
        doIt(filter, params, payload, cb);
        count ++;
    }, 20000); // kick off every 20 secs to make sure there is no retry
};

// Main control
if (input.operation == "countJobs") {
    countJobs(function(err) {
        if (err) console.log(err);
    });
} else if (input.operation == "resetJobs") {
    resetJobs(function(err) {
        if (err) console.log(err);
    });    
} else if (input.operation == "listJobs") {
    listJobs(function(err) {
        if (err) console.log(err);
    });    
} else if (input.operation == "cruiseControl") {
    cruiseControl(function(err) {
        if (err) console.log(err);
    });    
}
