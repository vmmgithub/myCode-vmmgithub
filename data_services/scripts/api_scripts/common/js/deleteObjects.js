#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility used for deleting objects from a collection (excluding a specific record)\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('c', 'source').describe('c', 'Source type [app.task, core.contact]')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalIds.id]').default('b', '_id')
    .alias('r', 'removeRel').describe('r', 'Remove hidden relations').boolean('r').default('r', false)
    .alias('f', 'file').describe('f', 'File to process')
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('e', 'exclude').describe('e', 'Exclude')
    .alias('d', 'recordLimit').describe('d', 'Record Limit per find request').default('d', 1000)
    .alias('o', 'operation').describe('o', 'Operation to perform [removeAll, removeById]')
    .alias('n', 'columnName').describe('n', 'Column name in the input file to use').default('n', '_id')
    .demand(['h', 't', 'c', 'o'])
    .argv;

var restApi = h.getAPI(input),
    sourceCollection = h.getCollection(restApi, input.source),
    tenantApi = restApi.setTenant(input.tenant),
    csvHelper = new csvHelperInstance();

var resultLength = 0;
var hiddenRel = 0;

var findObject = function(filter, params, callback) {
    h.findRecords(sourceCollection, {
        multiple: true,
        filter: filter,
        params: params,
    }, callback);
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

        var referers = [];
        async.eachLimit(res.data['core.link.references'], 5, function(ref, ocb) {
            findReferer(ref.type, ref._id, function(err, t) {
                if (t) referers.push(t);
                ocb(err);
            });
        },
        function(err) {
            callback(null, referers);
        });
    });
};
var removeReferers = function(rec, recKey, callback ) {

    var rels = rec.relationships;
    if (!rels || rels.length == 0 || !recKey ) return;

    collName = h.getCollection(restApi, rec.type);

    rec.relationships = _.reject(rels, function(r) { 
                      if (r.target && r.target.key && r.target.key == recKey) {
                            hiddenRel += 1;
                            h.log('info', 'Relationship details  Source: ' + rec.type + '::' + rec._id +' Target: ' + r.type + '::'+ r.target.key + ' Relationship:' + r.relation.name );
                      }
                      return r.target.key == recKey  
     });
     if (input.removeRel)
          collName.update(rec, callback);
};

var deleteObject = function(object, callback) {
    
    hiddenRel  = 0;

//    if (input.operation == 'log') {h.log('debug', ' Deleted ' + object._id); return callback();}

    getReferers(object, function(err, refs) {
               if (refs)  {
                    _.each(refs, function(refererRecord) {
                        removeReferers(refererRecord, object._id, callback); 
                    });
               } 

    if ((hiddenRel > 0) && (input.removeRel == false)) 
             h.log('info', 'Check log for relationship details; please re-run script with option --removeRel true');

    if ((hiddenRel > 0) && (input.removeRel == true)) {
             h.log('info', 'Removed hidden relationships, please re-run script to delete object');
             hiddenRel = 0;
    }

//    console.log("HiddenRel==>", hiddenRel);
    if (hiddenRel == 0 )  {
        sourceCollection.delete(object, function(err) {
        if (err)  {
             if (contains(JSON.stringify(err),'The record could not be deleted because it is related to other records.')) {
                deleteObject(object, callback);
             }
             else h.log('error', ' Deleting ' + object._id + ' ' + JSON.stringify(err));
        }
        else    h.log('debug', ' Deleted ' + object._id);
        });
    }
    callback();
  });

}

// ------------------------------------------------------------------------------------------------------
// Mode 1: Remove objects without knowing their _ids
// ------------------------------------------------------------------------------------------------------
if (input.operation == 'removeAll') {
    async.doUntil(
        // Worker function
    function(callback) {
        var filter = {};
        if (input.exclude) {
            if (!_.isArray(input.exclude)) input.exclude = [input.exclude];
            filter = {_id: {$nin: input.exclude}};
        }

        findObject(filter, {limit: input.recordLimit, columns: ["displayName"]}, function(err, records) {
            if (err) {
                h.log('error', err);
                return;
            }
            console.log("Records=>", records);
            async.eachLimit(records, input.limit, 
                function(rec, cb) {
                    deleteObject(rec, cb);
                },
                callback);    
        });
    },
    // when to stop
    function() { return resultLength < input.recordLimit},

    // exit out
    function(err, res) {
        if (err) 
            h.log('error', JSON.stringify(err));
        else 
            h.log('info', ' DONE');
    }
    );
}

// ------------------------------------------------------------------------------------------------------
// Mode 2: Remove objects with their _ids
// ------------------------------------------------------------------------------------------------------
if (input.operation == 'removeById') {
    csvHelper.readAsObj(input.file, function (data) {
        async.eachLimit(data, input.limit, function (csvRecord, callback) {
            if (!data) return callback();
            deleteObject({_id: csvRecord[input.columnName]}, callback);
        },
        function (err) {
            h.log('info', " DONE " + err);
        });
    });
}
