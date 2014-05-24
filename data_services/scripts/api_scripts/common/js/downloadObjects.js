#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var jsonpath = require('JSONPath').eval;
var inflection = require('inflection');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");
var RestApiInterface = require('../../lib/helpers/RestApi');
var h = require('../../common/js/helper');

var input = require('optimist')
    .usage('\nREADME: This is a utility to export for any object in Renew, using a CSV input.\
        \n The data can be downloaded using -b or --searchBy option example -b {"_id":"52fe2973386028910a00b858"}\
        \n \
        \n  Note : Add single quote around the curly brackets( {} )   . \
        \n \
        \n The CSV file supports _id, displayName and externalIds.id columns in the file  example :\
        \n _id\
        \n 52fe2973386028910a00b858\
        \n 52fe2973386028910a00b839\
        \n \
        \n The columns (-l)  can be specified as single column, relationships or array examples : \
        \n SingleColumn : _id or displayName \
        \n Relationships  :  relationships.salesRep \
        \n Array  : externalIds[id,_ids,schemeName.name]  or attachedQuotes[documentName] or externalIds[_id]  column name should be separated by comma\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('n', 'port').describe('n', 'Specify port').default('n', '443')
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password')
    .alias('l', 'columns').describe('l', 'List of columns for download in the dot notation. Multiple columns can be retreived by repeating this param')
    .alias('s', 'source').describe('s', 'Source type').default('s', 'app.opportunity')
    .alias('f', 'file').describe('f', 'File Name')
    .alias('d', 'db').describe('d', 'Store results to mysql instead of file output').boolean('d').default('d', false)
    .alias('b', 'searchBy').describe('b', 'String version of JSON filter').default('b', '{}')
    .alias('g', 'log').describe('g', 'Log output into text vs sql, applicable only when db = true').boolean('g').default('g', false)
    .alias('e', 'exportFile').describe('e', 'File Name of the file from Renew with the JSON content')
    .demand(['h', 't', 's',  'l'])
    .argv;

if (!_.isArray(input.columns)) input.columns = [input.columns];

var restApi = h.getAPI(input),
    csvHelper = new csvHelperInstance(),
    sourceCollection = h.getCollection(restApi, input.source),
    connectionHelper = new h.ConnectionHelper(input),
    _stats = new h.Stats('downloadObjects', input.source, 'Downloading ' + JSON.stringify(input.columns) + ' with filter ' + input.searchBy);

// Globals
var allColumnNames;
var globalSearchBy;
var globalFields;

var findSource = function (searchBy, value, callback) {
    h.findRecords(sourceCollection, {
        multiple: true,
        searchBy: searchBy,
        value: value,
    }, callback);
};

var scanObjects = function (callback) {
    var filter = {};
    if (input.searchBy) {
      try {
        filter = JSON.parse(input.searchBy);
      } catch(err) {
        h.log('error', err);
      }
    } else {
      if (input.displayName) filter.displayName = input.displayName;
      if (input.tags) filter.tags = input.tags;
    }

    h.findRecords(sourceCollection, {
        filter: filter,
        stream: true,
        file: input.exportFile,
        columns: input.columns,
        ignoreEmpty: true,
    }, callback, writeObj);
};

var writeObj = function(obj, cb) {
    _stats.incRecords();
    var vals = [];
    _.each(input.columns, function(path) {
        vals.push(h.getObjectValueFromPath(obj, path));
    });

    if (!input.db) {
        var s = '';
        _.each(vals, function(val) {
            s+= '"' + val + '",';
        });
        console.log(s);
        return;
    }

    connectionHelper.insertRecord(input.source, input.columns, vals, function(err) {
        if (err) { 
            h.log('error', 'trouble insert records ' + err);
            _stats.incErrors();
        }
    });
};

var init = function(cb) {
    if (!input.db) {
        var s = '';
        _.each(input.columns, function(path) {
            s+= '"' + path + '",';
        });

        console.log(s);
        return cb();
    }

    connectionHelper.initConnection(function(err, conn) {
        connectionHelper.createTable(input.source, input.columns, false, cb)
    });
};

var close = function(err) {
    if (err) h.log('info', 'Done ' + JSON.stringify(err));
    _stats.markComplete(err);

    if (input.db) connectionHelper.closeConnection(_stats, function(err) {
        //DONE
    });
}

var processRecord = function (searchBy, searchByValue, callback) {
    var done = function(err) {
        if (err) h.log('error', "Skipping  object '" + searchByValue + "': " + JSON.stringify(err));
        callback();
    };
    findSource(searchBy.fieldName, searchByValue, function(err, res) {
        if (err) return done(err);
       
       async.eachLimit(res, 1, function(obj, cb) {
            writeObj(obj);
        });
       callback && callback();
     });
   done;
};

var getDataType = function(columnName) {
    var datatype ='string';
    var chops = columnName.split('(');
    var fieldName = chops[0];
    if (chops.length > 1)
        datatype = chops[1].split(')')[0];

    return {
        datatype: datatype,
        fieldName: fieldName
    }
}

init(function(err) {
    if (err) return h.log('error', err);

    if (!input.file)  {
       scanObjects(close);
    };

    if (input.file) {
        csvHelper.readAsObj(input.file, function (data) {
            async.eachLimit(data, 5, function (csvRecord, callback) {
                if (!data) return callback();

                if (!allColumnNames) {
                    allColumnNames = _.keys(csvRecord);     // contains column names and datatype
                    globalSearchBy = getDataType(_.first(allColumnNames));    //  Identify searchBy(first column) from all
                }

                var searchByValue = _.first(_.values(csvRecord));
                if (globalSearchBy.fieldName == '_id' || globalSearchBy.fieldName == 'externalIds.id' || globalSearchBy.fieldName == 'displayName' ) {
                    processRecord(globalSearchBy, searchByValue, callback);
                } else {
                    h.log('warn', 'Skipping ' + csvRecord);
                    callback();
                }
            },
           close);
       });
    }    
});
