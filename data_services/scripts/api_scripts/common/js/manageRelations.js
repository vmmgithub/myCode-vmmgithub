#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility to add or remove relationships for any object in Renew, using a CSV input.\
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
    .alias('s', 'source').describe('s', 'Source type').default('s', 'app.opportunity')
    .alias('r', 'relationship').describe('r', 'Relationship to modify').default('r', 'salesRep')
    .alias('d', 'target').describe('d', 'Target type').default('d', 'core.contact')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalIds.id]').default('b', '_id')
    .alias('c', 'searchByTarget').describe('c', 'Search by attribute [_id, displayName, externalIds.id]').default('c', '_id')
    .alias('v', 'forReal').describe('v', 'if running in test mode or for real').boolean('v').default('v', false)
    .alias('o', 'operation').describe('o', 'Operation to perform [add, remove]').default('o', 'add')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    sourceCollection = h.getCollection(restApi, input.source),
    targetCollection = h.getCollection(restApi, input.target),
    csvHelper = new csvHelperInstance();

var findSource = function (value, callback) {
    h.findRecords(sourceCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var findTarget = function (value, callback) {
    h.findCachedRecords(targetCollection, {
        searchBy: input.searchByTarget,
        value: value,
    }, callback);
};

var logRelationship = function(sourceRecord) {
    var found = 0;
    _.each(sourceRecord.relationships, function (rel) {
        if(rel.relation && rel.relation.name == input.relationship) {
            h.log('debug', "EXISTING :" + sourceRecord.displayName + "," + rel.target.displayName + "," + sourceRecord._id);
            found++;
        }
    });

    //Prints a line when no relationships exist for an object
    if (found == 0) {
         h.log('debug', "EXISTING :" + sourceRecord.displayName + ",," +sourceRecord._id);
    }
}

//     One level nested relationship is supported
var addRelationship = function (sourceRecord, targetRecord, callback) {
    var rels = sourceRecord.relationships;
    var relName = input.relationship;

    if (h.contains(relName, '.')) {
        var parentRelName = relName.split('.')[0];
        relName = relName.split('.')[1];

        var parentRel = _.find(rels, function(r) { return r.relation.name == parentRelName});
        if (parentRel && !parentRel.relationships) {
            parentRel.relationships = [];
            rels = parentRel.relationships;
        } else if (!parentRel) {
            var newParentRel = {
                relation: {name: parentRelName},
                relationships: []
            };
            rels.push(newParentRel);
            rels = newParentRel.relationships;
        } else {
            rels = parentRel.relationships;
        }
    }

    rels.push({
        target: h.getTargetPointer(targetRecord),
        relation: {
            name: relName
        },
        type: "core.relationship"
    });

    if (input.forReal)
        sourceCollection.update(sourceRecord, callback);
    else
        callback(null, sourceRecord);
};

var removeRelationship = function(sourceRecord, callback) {
    var rels = sourceRecord.relationships;
    var relName = input.relationship;

    if (h.contains(relName, '.')) {
        var parent = relName.split('.')[0];
        relName = relName.split('.')[1];
        var r = _.find(rels, function(r) { return r.relation.name == parent});

        if (!r || !r.relationships) return callback(null, sourceRecord);
        rels = r.relationships;
        r.relationships = _.reject(r.relationships, function (rel) {
            return rel.relation && rel.relation.name == relName
        });
    } else {
        sourceRecord.relationships = _.reject(rels, function (rel) {
            return rel.relation && rel.relation.name == relName
        });
    }

    if (input.forReal)
        sourceCollection.update(sourceRecord, callback);
    else
        callback(null, sourceRecord);
}

var processRecord = function (sourceName, targetName, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "" + (input.forReal ? 'for real': 'test run') + ' ' + (input.operation) + ' ' + input.relationship + " on record " + sourceName + ": " + JSON.stringify(err));
        } else  {
            h.log('info', "" + (input.forReal ? 'for real': 'test run') + ' ' + (input.operation) + ' ' + input.relationship + " on record " + sourceName + " with " + targetName);
        }

        callback();
    };

   	findSource(sourceName, function(err, res) {
         if (err) return done(err);     // Skipping if object does not exists

        async.eachLimit(res, 1, function(sourceRecord, ocb) {
            logRelationship(sourceRecord);

            if (input.operation == 'add') {
                findTarget(targetName, function(err, targetRecord) {
                    if (err) return done(err);
                    addRelationship(sourceRecord, targetRecord, ocb);
                });
            } else if (input.operation == 'remove') {
                removeRelationship(sourceRecord, ocb);
            } else if (input.operation == 'log') {
                logRelationship(sourceRecord, ocb);
                callback();          // added  callback to move to next record
            }
        },
        done);
    });
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();

        var sourceName = csvRecord["Source"];
        var targetName = csvRecord["Target"];

        if (sourceName && (input.operation == 'remove' || input.operation == 'log' || input.operation == 'add' && targetName)) {
            processRecord(sourceName, targetName, callback);
        } else {
            h.log('Skipping ' + sourceName + ' and ' + targetName);
            callback();
        }
    },
    function (err) {
        h.log('info', 'DONE ' + err);
    });
});


