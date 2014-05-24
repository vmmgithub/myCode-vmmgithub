#!/usr/bin/env node
 
var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");
 
var input = require('optimist')
    .usage('\nREADME: This is a utility to finds and replaces all relationships for any object in Renew, using a CSV input.\
       \nThe input file should contain two columns, "Source" column with the source object displayName and "Target" with target object displayName\
       \n Sample CSV File layout with example of _id column for searchBy and searchByTarget fields  \
       \n ========================================================\
       \n Source,Target\
       \n 52d92e8c36d0cfe25f0119a5,50d172241506efaa6800116b\
       \n source is what you want to change\
       \n target is what the source will receive\
       \n e.g., updating an opportunity with a new reseller, the opportunity _id is the source,\
       \n and the reseller _id is the target.\
       \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('n', 'port').describe('n', 'Specify port')
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password')
    .alias('f', 'file').describe('f', 'File to process')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('s', 'source').describe('s', 'Source type').default('s', 'core.contact')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalIds.id]').default('b', '_id')
    .alias('r', 'relationships').describe('r', 'only replace for certain relationships [salesRep]')
    .alias('v', 'forReal').describe('v', 'if running in test mode or for real').boolean('v').default('v', false)
    .demand(['h', 't', 'f'])
    .argv;
 
var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    sourceCollection = h.getCollection(restApi, input.source),
    csvHelper = new csvHelperInstance();
 
if (!input.relationships) input.relationships = [];
if (!_.isArray(input.relationships)) input.relationships = [input.relationships];
 
var findSource = function (value, callback) {
    h.findRecords(sourceCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
        columns: ['_id', 'displayName', 'type']
    }, callback);
};
 
var findTarget = function (value, callback) {
    h.findRecords(sourceCollection, {
        searchBy: input.searchBy,
        value: value,
        columns: ['_id', 'displayName', 'type']
    }, function(err, res) {
        if (res && res.length == 1) return callback(null, res[0]);
       
        return callback(err, res);
    });
};
 
var findReferer = function (type, id, callback) {
    var coll = h.getCollection(restApi, type);
 
    h.findRecords(coll, {
        searchBy: '_id',
        value: id,
        columns: ['_id', 'displayName', 'type', 'relationships', 'systemProperties']
    }, function(err, res) {
        if (res && res.length == 1) return callback(null, res[0]);
 
        return callback(err, res);
    });
};
 
var getReferers = function (sourceRecord, callback) {
    tenantApi.execute(h.getCollectionName(input.source), sourceRecord._id, 'referers', {}, function(err, res) {
        if (err || res.success != true || !res || !res.data)
            return callback("on referers call " + JSON.stringify(err || res));
 
        if (_.isEmpty(res.data['core.link.references'])) return callback(null, []);
        h.log('debug', 'Got back ' + res.data['core.link.references'].length + ' references for ' + sourceRecord._id);
 
        var referers = [];
        async.eachLimit(res.data['core.link.references'], 5, function(ref, ocb) {
            findReferer(ref.type, ref._id, function(err, t) {
                console.log("T ===>  ", t._id, "   ", t.type );
                if (t) referers.push(t);
                ocb(err);
            });
        },
        function(err) {
            callback(err, referers);
        });
    });
};
 
var swapKeys = function(rec, rels, oldKey, newKey) {
    if (!rels || rels.length == 0 || !oldKey || !newKey) return;
 
    _.each(rels, function(rel) {
        // If only using for a subset of relationships
        if (!_.isEmpty(rel.relationships) && !_.find(input.relationships, rel.relation && rel.relation.name)) return;
 
        if (rel.target && rel.target.key && rel.target.key == oldKey) {
            rel.target.key = newKey;
            h.log('debug', 'Replacing ' + rel.relation.name + ' on ' + rec.type + '::' + rec._id + ' from ' + oldKey + ' to ' + newKey);
        }
    });
};
 
var replaceRelationship = function (sourceRecord, targetRecord, refererRecord, callback) {
    var rels = refererRecord.relationships;
    var oldKey = sourceRecord && sourceRecord._id;
    var newKey = targetRecord && targetRecord._id;
    var refererCollection =  h.getCollection(restApi, refererRecord.type);
 
    // parent relationships
    swapKeys(refererRecord, rels, oldKey, newKey);
 
    // nested relationships
    _.each(rels, function(r) {
        swapKeys(refererRecord, r.relationships, oldKey, newKey);
    });
 
    if (input.forReal)
        refererCollection.update(refererRecord, callback);
    else
        callback(null, refererRecord);
};
 
var processRecord = function (sourceName, targetName, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', '' + (input.forReal ? 'for real': 'test run') + " on record " + sourceName + ": " + JSON.stringify(err));
        } else  {
            h.log('info', '' + (input.forReal ? 'for real': 'test run') + " on record " + sourceName + " with " + targetName);
        }
 
        callback();
    };
 
                findSource(sourceName, function(err, res) {
         if (err) return done(err);     // Skipping if object does not exists
 
        async.eachLimit(res, 1, function(sourceRecord, ocb) {
            findTarget(targetName, function(err, targetRecord) {
                if (err) return done(err); // Skipping if object does not exists
 
                var e, refCount = 1;
                async.until(
                    function() {
                        if (e || !refCount) h.log('info', 'Done with this record ' + sourceRecord._id);
                        return e || !refCount;
                    },
                    function(icb) {
                        getReferers(sourceRecord, function(err, refs) {
                            // set variables for the until truth test
                            e = err;
                            refCount = refs && refs.length;
 
                            if (err || !refs || refs.length == 0) return ocb();
 
                            var innerDone = _.after(refs.length, function(){
                                _.delay(icb,  2 * 60 * 1000); // wait for 30 seconds                           
                            });
 
                            _.each(refs, function(refererRecord) {
                                replaceRelationship(sourceRecord, targetRecord, refererRecord, innerDone);
                            });
                        });
                    },
                    ocb);
 
            });
        },
        done);
    });
};
 
h.log('info','Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();
 
        var sourceName = csvRecord["Source"];
        var targetName = csvRecord["Target"];
 
        if (sourceName && targetName) {
            processRecord(sourceName, targetName, callback);
        } else {
            h.log('warn', 'Skipping ' + sourceName + ' and ' + targetName);
            callback();
        }
    },
    function (err) {
        h.log('info', "DONE " + err);
    });
});
 
