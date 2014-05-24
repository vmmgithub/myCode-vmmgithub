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
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('s', 'source').describe('s', 'Source type').default('s', 'core.contact/organization')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalIds.id]').default('b', '_id')
    .alias('r', 'relationships').describe('r', 'only replace for certain relationships [customer]')
    .alias('v', 'forReal').describe('v', 'if running in test mode or for real').boolean('v').default('v', false)
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
            h.log('debug', 'INIT: need to scan ' + collName + ' for ' + collapsedRels[collName].filteredRels);
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
        columns: ['_id', 'type', 'relationships', 'systemProperties']
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

var findAndRepointReferers = function (refererType, relation, sourceId, replacerFn, callback) {
    var refererColl = h.getCollection(restApi, refererType),
        filter = {};
    filter['relationships.' + relation + '.targets.key'] = sourceId;

    h.findRecords(refererColl, {
        multiple: true,
        ignoreEmpty: true,
        stream: true,
        callIteratorAtEnd: true,
        filter: filter,
        columns: ['_id', 'relationships', 'type', 'systemProperties']
    }, callback, replacerFn);
};

var swapKeys = function(rec, rels, oldKey, newKey) {
    if (!rels || rels.length == 0 || !oldKey || !newKey) return;

    _.each(rels, function(rel) {
        // If only using for a subset of relationships
        if (!_.isEmpty(rel.relationships) && !_.find(input.relationships, rel.relation && rel.relation.name)) return;

        if (rel.target && rel.target.key && rel.target.key == oldKey) {
            rel.target.key = newKey;
            h.log('debug', (input.forReal ? 'Replacing ' : 'Found ') + rel.relation.name + ' on ' + rec.type + ':' + rec._id + ' from ' + oldKey + ' to ' + newKey);
        }
    });
};

var replaceRelationship = function (context, callback) {
    var refererRecord = context.refererRecord;
    if (!refererRecord) return callback();

    var rels = refererRecord.relationships;
    var oldKey = context.sourceRecord && context.sourceRecord._id;
    var newKey = context.targetRecord && context.targetRecord._id;
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
            h.log('error', (input.forReal ? 'for real': 'test run') + " on record " + sourceName + ": " + JSON.stringify(err));
        } else  {
            h.log('info', (input.forReal ? 'for real': 'test run') + " on record " + sourceName + " with " + targetName);
        }

        callback();
    };


   	findRecord(sourceName, function(err, sourceRecord) {
        if (err || !sourceRecord) return done(err || 'source not found');     // Skipping if object does not exists

        findRecord(targetName, function(err, targetRecord) {
            if (err || !targetRecord) return done(err || 'target not found'); // Skipping if object does not exists

            // First check how many referers exist, if less than 50, update them in one shot
            // If not, then scan the world
            getReferers(sourceRecord, function(err, refs) {
                if (err || !refs || refs.length == 0) return done(err);

                //None or handful of refs exist
                if (refs.length < 50) {
                    var referers = [];
                    async.eachLimit(refs, input.limit, function(ref, ocb) {
                        findReferer(ref.type, ref._id, function(err, refererRecord) {
                            if (err || !refererRecord) return ocb(err); 
                            replaceRelationship({sourceRecord: sourceRecord, targetRecord: targetRecord, refererRecord: refererRecord}, ocb);
                        });
                    },
                    done);
                } else {

                    // Go through each relationship config
                    async.eachLimit(REL_CONFIGS, 1, function(relConfig, icb) {

                        h.log('debug', sourceRecord._id + ' scanning reference type ' + relConfig.source);
                        //Go through each relationship name per config
                        async.eachLimit(relConfig.relations, 1, function(relation, iicb) {
                            h.log('debug', sourceRecord._id + ' scanning reference type ' + relConfig.source + ' for ' + relation + ' referenences');

                            var calledBack = false;
                            var q = async.queue(replaceRelationship, input.limit);
                            q.drain = function(err) {
                                if (!calledBack) { calledBack = true; iicb(err); }
                            };

                            // Get all referers for this relation source and relation name
                            findAndRepointReferers(relConfig.source, relation, sourceRecord._id, function(refererRecord) {
                                q.push({sourceRecord: sourceRecord, targetRecord: targetRecord, refererRecord: refererRecord}, function(err) {
                                    if (err) h.log('error', 'saving referer record ' + err);
                                });
                            }, function(err) {
                                if (!calledBack) { calledBack = true; iicb(err); }
                            });
                        }, icb);
                    }, done);

                }
            });
        });
    });
};

h.log('info','Processing ' + input.file);
init(function(err) {
    if (_.isEmpty(REL_CONFIGS)) return h.log('error' ,'No configs present, missing the type attribute in source? Ex. core.contact/organization');
    if (input.source == 'core.contact/person') return h.log('error' ,'No support for person dedup currently.');

    csvHelper.readAsObj(input.file, function (data) {
        async.eachLimit(data, 1, function (csvRecord, callback) {
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
});

