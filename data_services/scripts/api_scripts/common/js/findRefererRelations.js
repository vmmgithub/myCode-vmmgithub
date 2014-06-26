#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility to finds all inverse references for a particular object.\
       \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('n', 'port').describe('n', 'Specify port')
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password')
    .alias('f', 'file').describe('f', 'File to process')
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('s', 'source').describe('s', 'Source type').default('s', 'core.contact/organization')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalIds.id]').default('b', '_id')
    .alias('r', 'relationships').describe('r', 'only bring back for certain relationships [salesRep]')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    sourceCollection = h.getCollection(restApi, input.source),
    csvHelper = new csvHelperInstance();

if (!input.relationships) input.relationships = [];
if (!_.isArray(input.relationships)) input.relationships = [input.relationships];

var findRelTypes = function (callback) {
    var relTypeCollection = h.getCollection(restApi, 'core.relationship.types');
    var filter = {
            $or: [{'relations.target' : input.source }, {'relations.relations.target' : input.source}]
        };

    h.findRecords(relTypeCollection, {
        multiple: true,
        filter: filter,
        columns: [ 'source', 'relations']
    }, function(err, relConfigs) {
        if (err || !relConfigs) return callback(err, relConfigs);

        // remove junk rel pointers 
        relConfigs = _.reject(relConfigs, function(relConfig) { 
            return relConfig.source == 'core.activity' || h.contains(relConfig.source, 'input') || h.contains(relConfig.source, '.snapshot')
        });

        var collapsedRels = {};
        _.each(relConfigs, function(relConfig) {
            var collName = h.getCollectionName(relConfig.source);
            if (!collapsedRels[collName]) collapsedRels[collName] = {source: collName, filteredRels: []}

            // scan first level relations to get only items of interest
            _.each(relConfig.relations, function(relation) {
                var match = false;
                if (relation.target == input.source) collapsedRels[collName].filteredRels.push(relation.name.name);

                // scan second level relations to get only items of interest
                // API does not support search by sub relationship, so ignoring these
                // Fix when Renew supports this functionality
                /*
                _.each(relation.relations, function(innerRelation) {
                    if (innerRelation.target == input.source) filteredRels.push(relation.name.name + '.' + innerRelation.name.name);
                });
                */
            });
            collapsedRels[collName].filteredRels = _.uniq(collapsedRels[collName].filteredRels);
            //h.log('debug', 'INIT: need to scan ' + collName + ' for ' + collapsedRels[collName].filteredRels);
        });

        var r = _.map(_.keys(collapsedRels), function(c) { return {source: c, relations: collapsedRels[c].filteredRels}});
        callback(err, r);
    });
};

var REL_CONFIGS;
var init = function(callback) {
    findRelTypes(function(err, relConfigs) {
        REL_CONFIGS = relConfigs;
        return callback(err);
    });
};

var findRecord = function (value, callback) {
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
        columns: ['_id', 'type', 'relationships']
    }, function(err, res) {
        if (res && res.length == 1) return callback(null, res[0]);

        return callback(err, res);
    });
};

//Use this to do a fast scan and avoid spamming the entire collection set
var getReferers = function (sourceRecord, callback) {
    tenantApi.execute(h.getCollectionName(input.source), sourceRecord._id, 'referers', {}, function(err, res) {
        if (err || res.success != true || !res || !res.data) 
            return callback("on referers call " + JSON.stringify(err || res));

        return callback(null, res.data['core.link.references']);
    });
};

var findAndPrintReferers = function (refererType, relation, sourceId, replacerFn, callback) {
    var refererColl = h.getCollection(restApi, refererType),
        filter = {};
    filter['relationships.' + relation + '.targets.key'] = sourceId;

    h.findRecords(refererColl, {
        multiple: true,
        ignoreEmpty: true,
        stream: true,
        filter: filter,
        columns: ['_id', 'relationships', 'type', 'systemProperties']
    }, callback, replacerFn);
};

var printKeys = function(rec, rels, oldKey) {
    if (!rels || rels.length == 0 || !oldKey) return;

    _.each(rels, function(rel) {
        // If only using for a subset of relationships
        if (!_.isEmpty(rel.relationships) && !_.find(input.relationships, rel.relation && rel.relation.name)) return;

        if (rel.target && rel.target.key && rel.target.key == oldKey) {
            h.log('debug', 'Found ' + rel.relation.name + ' on ' + rec.type + ':' + rec._id + ' for ' + oldKey);
        }
    });
};

var scanRelationship = function (refererRecord, sourceRecord) {
    if (!refererRecord) return callback();

    var rels = refererRecord.relationships;
    var oldKey = sourceRecord && sourceRecord._id;
    var refererCollection =  h.getCollection(restApi, refererRecord.type);

    // parent relationships
    printKeys(refererRecord, rels, oldKey);

    // nested relationships
    _.each(rels, function(r) {
        printKeys(refererRecord, r.relationships, oldKey);
    });
};


var processRecord = function (sourceName, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "scan on record " + sourceName + ": " + JSON.stringify(err));
        } else  {
            h.log('info', "scan on record " + sourceName);
        }

        callback();
    };

    findRecord(sourceName, function(err, sourceRecord) {
        if (err || !sourceRecord) return done(err || 'source not found');     // Skipping if object does not exists

        // First check how many referers exist, if less than 50, update them in one shot
        // If not, then scan the world
        getReferers(sourceRecord, function(err, refs) {
            if (err || !refs || refs.length == 0) return done(err);

            //None or handful of refs exist
            if (refs.length < 50) {
                async.eachLimit(refs, input.limit, function(ref, ocb) {
                    findReferer(ref.type, ref._id, function(err, refererRecord) {
                        if (err || !refererRecord) return ocb(err); 
                        scanRelationship(refererRecord, sourceRecord);
                        ocb();
                    });
                },
                done);
            } else {

                h.log('warn', sourceRecord._id + 'found more than 50 records for ');
                // Go through each relationship config
                async.eachLimit(REL_CONFIGS, 1, function(relConfig, icb) {

                    h.log('debug', sourceRecord._id + ' scanning reference type ' + relConfig.source);
                    //Go through each relationship name per config
                    async.eachLimit(relConfig.relations, 1, function(relation, iicb) {
                        h.log('debug', sourceRecord._id + ' scanning reference type ' + relConfig.source + ' for ' + relation + ' referenences');

                        var calledBack = false;

                        // Get all referers for this relation source and relation name
                        findAndPrintReferers(relConfig.source, relation, sourceRecord._id, function(refererRecord) {
                            scanRelationship(refererRecord, sourceRecord);
                        }, function(err) {
                            if (!calledBack) { calledBack = true; iicb(err); }
                            else {console.log('issue');}
                        });
                    }, icb);
                }, done);

            }
        });
    });
};

h.log('info','Processing ' + input.file);
init(function(err) {
    if (_.isEmpty(REL_CONFIGS)) return h.log('error' ,'No configs present, missing the type attribute in source? Ex. core.contact/organization');

    csvHelper.readAsObj(input.file, function (data) {
        async.eachLimit(data, 1, function (csvRecord, callback) {
            if (!data) return callback();

            var sourceName = csvRecord["Source"];

            if (sourceName) {
                processRecord(sourceName, callback);
            } else {
                h.log('warn', 'Skipping ' + sourceName);
                callback();
            }
        },
        function (err) {
            h.log('info', "DONE " + err);
        });
    });
});
