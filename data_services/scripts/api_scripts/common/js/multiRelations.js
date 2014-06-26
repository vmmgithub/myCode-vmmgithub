#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility to add or remove relationships for any object in Renew, using a CSV input.\
       \n Sample CSV File layout with example of _id column for searchBy and searchByTarget fields  \
       \n ========================================================\
        \n_id(string),relationships.salesRep(core.contact/person),relationships.customer(core.contact/organization) \
       \n 52d92e8c36d0cfe25f0119a5,50d172241506efaa6800116b,50d172241506efaa682222cb\
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
    .alias('v', 'forReal').describe('v', 'if running in test mode or for real').boolean('v').default('v', false)
    .alias('o', 'operation').describe('o', 'Operation to perform [add, remove, replace]').default('o', 'add')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    sourceCollection = h.getCollection(restApi, input.source),
    csvHelper = new csvHelperInstance();

// Globals
var globalSearchBy;
var globalRelNames;

var findSource = function (value, callback) {
    h.findRecords(sourceCollection, {
        multiple: input.multiple,
        searchBy: globalSearchBy,
        value: value,
    }, callback);
};

var findTarget = function (coll, value, callback) {
    var targetCollection = h.getCollection(restApi, coll);

    h.findCachedRecords(targetCollection, {
        searchBy: globalSearchBy,
        value: value,
    }, callback);
};

var logRelationship = function(prefix, sourceRecord, relName) {
    h.log('debug', prefix + sourceRecord._id + "|" + relName + "|" + h.getRelKeys(sourceRecord, relName));
};

//     One level nested relationship is supported
var addRelationship = function (sourceRecord, targetRecord, relName) {
    var rels = sourceRecord.relationships;

    rels.push({
        target: h.getTargetPointer(targetRecord),
        relation: {
            name: relName
        },
        type: "core.relationship"
    });
};

var removeRelationship = function(sourceRecord, relName) {
    var rels = sourceRecord.relationships;

    sourceRecord.relationships = _.reject(rels, function (rel) {
        return rel.relation && rel.relation.name == relName
    });
};

var replaceRelationship = function (sourceRecord, targetRecord, relName) {
    removeRelationship(sourceRecord, relName);
    addRelationship(sourceRecord, targetRecord, relName);
};

var processRecord = function (sourceName, relNames, relValues, callback) {
    var done = function(err) {
        if (err) 
            h.log('error', "" + (input.forReal ? 'for real': 'test run') + ' ' + (input.operation) + " on record " + sourceName + ": " + JSON.stringify(err));
        else 
            h.log('info', "" + (input.forReal ? 'for real': 'test run') + ' ' + (input.operation) + " on record " + sourceName + " with " + relValues);
        return callback();
    };

    var updateRecord = function(sourceRecord, cb) {
        if (!input.forReal) return cb();
        sourceCollection.update(sourceRecord, cb);
    };

   	findSource(sourceName, function(err, res) {
         if (err) return done(err);     // Skipping if object does not exists

        async.eachLimit(res, 1, function(sourceRecord, ocb) {
            var i = -1; 
            async.eachLimit(relNames, 1, function(rel, cb) {
                i++;

                if (input.operation == 'add' || input.operation == 'replace') {
                    if (!relValues[i]) return cb();

                    findTarget(rel.type, relValues[i], function(err, targetRecord) {
                        if (err) return cb(err); // Skipping if object does not exists

                        logRelationship('EXISTING: ', sourceRecord, rel.relName);
                        if (input.operation == 'add') addRelationship(sourceRecord, targetRecord, rel.relName);
                        if (input.operation == 'replace') replaceRelationship(sourceRecord, targetRecord, rel.relName);
                        logRelationship('REPLACEM: ', sourceRecord, rel.relName);

                        if (i+1 == relNames.length) 
                            return updateRecord(sourceRecord, cb);
                        else 
                            cb();
                    });
                } else if (input.operation == 'remove') {
                    logRelationship('EXISTING: ', sourceRecord, rel.relName);
                    removeRelationship(sourceRecord, rel.relName);
                    logRelationship('REPLACEM: ', sourceRecord, rel.relName);

                    if (i+1 == relNames.length) 
                        return updateRecord(sourceRecord, cb);
                    else 
                        cb();
                }
            }, ocb);
        },
        done);
    });
};

var getFieldDef = function(columnName) {
    if (!columnName) return;

    var type;
    var chops = columnName.split('(');
    var relName = chops[0];
    if (chops.length > 1) 
        type = chops[1].split(')')[0];

    if (h.contains(relName, 'relationships.')) relName = relName.split('.')[1];
    return {
        type: type,
        relName: relName
    }
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    if (!data) return;

    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!globalSearchBy) {
            var allColumnNames = _.keys(csvRecord);     // contains column names and datatype
            globalSearchBy = getFieldDef(_.first(allColumnNames)).relName;    //  Identify searchBy(first column) from all
            globalRelNames = _.compact(_.map(_.rest(allColumnNames, 1), getFieldDef));  // building Object  of string and field Name
        }

        var relValues = _.rest(_.values(csvRecord), 1);
        var searchByValue = _.first(_.values(csvRecord));   

        if (searchByValue && (input.operation == 'remove' || input.operation == 'replace' || input.operation == 'add')) {
            processRecord(searchByValue, globalRelNames, relValues, callback);
        } else {
            h.log('Skipping ' + searchByValue + ' and ' + targetName);
            callback();
        }
    },
    function (err) {
        h.log('info', 'DONE ' + err);
    });
});


