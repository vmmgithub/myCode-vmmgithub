#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var moment = require("moment");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");
var jsonpath = require('JSONPath').eval;

var input = require('optimist')
    .usage('\nREADME: This is a utility to update or remove attributes for any object in Renew, using a CSV input.\
        \nThe input file can contain multiple attributes. First cloumn provides search criteria and can have value like _id, displayname, externalIds.id \
        \n all other columns (attributes) are separated by  comma and should have complete path (dot notation) and datatype within () like  extensions.master.batchclientBatchQuarter.value(string). \
        \n Sample File Layout: \
        \n_id(string),amount.amount(number),extensions.master.batchQuarter.value(string),extensions.master.clientBatchQuarter.value(string) \
        \n52a9926cd2bc67394a000159,2251.90,Q2 2014,FY14Q21 \
        \n52a9926cd2bc67394a000159,2251.90,,FY14Q21 \
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('n', 'port').describe('n', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('b', 'updBlank').describe('b', 'When set to true, update field as blank when value not provided ').boolean('b').default('b', false)
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('s', 'source').describe('s', 'Source type Ex. app.opportunity').default('s', 'app.opportunity')
    .alias('o', 'operation').describe('o', 'Operation to perform [update, delete]').default('o', 'update')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    sourceCollection = h.getCollection(restApi, input.source),
	csvHelper = new csvHelperInstance();

// Globals
var allColumnNames;
var globalSearchBy;
var globalFields;

var findSource = function (searchBy, value, callback) {
    h.findRecords(sourceCollection, {
        multiple: input.multiple,
        searchBy: searchBy,
        value: value,
    }, callback);
};

var updateAttribute = function (sourceRecord, fieldName, datatype, fieldValue, callback) {

    if (input.operation == 'update') {

        // Check format and set appropriate type
        if (datatype == 'number') {
            fieldValue = parseFloat(fieldValue); 
            if (!_.isNumber(fieldValue)) return callback();
        }

        if (datatype == 'boolean') {
            fieldValue = (fieldValue === 'true' || fieldValue === 'TRUE');
            if (!_.isBoolean(fieldValue)) return callback();
        }

        if (datatype== 'date') {
            var t = moment(fieldValue);
            if (!t.isValid()) return callback();
            if (t.hours() == 0) t.hours(12);
            fieldValue = t.toISOString();
        }

        if (fieldName == 'externalIds.id') {
            sourceRecord.externalIds = _.reject(sourceRecord.externalIds, function(xid) {return xid.id = fieldValue});
            sourceRecord.externalIds.push({id: fieldValue, schemeId: {name: 'manuallyAdedId'}});

        } else if (h.endsWith(fieldName, '.name')) {
            var d = h.getLookup(fieldName, fieldValue);
            h.deepSet(sourceRecord, fieldName, d && d.name || fieldValue, input.operation);
			
        } else if (!h.startsWith(fieldName, 'flows')) {
        /*  
            else if (h.startsWith(fieldName, 'flows')) {
            var flowName = fieldName.split('.')[1];
            var targetState = fieldValue;
            var result = fieldValue.replace( /([A-Z])/g, " $1" );
            var targetStateDisplayName = result.charAt(0).toUpperCase() + result.slice(1);
            var existingState = sourceRecord.flows[flowName].state.name;
            var prevChangeDate = _.last(sourceRecord.flows[flowName].transitions).changeDate;

            sourceRecord.flows[flowName].state = {
                name: targetState,
                displayName: targetStateDisplayName
            };
            sourceRecord.flows[flowName].transitions.push({
                toState: targetState,
                fromState: existingState,
                timeGap: moment(prevChangeDate).diff(moment()),
                changeDate: moment().toISOString(),
            });
        
        }
         */ 
	       h.deepSet(sourceRecord, fieldName, fieldValue, input.operation);
        } 
    } else if (input.operation == 'delete') {
        if (fieldName == 'externalIds.id') {
            sourceRecord.externalIds = _.reject(sourceRecord.externalIds, function(xid) {return xid.schemeId.name= 'manuallyAdedId'});
        } else {  
	       h.deepSet(sourceRecord, fieldName, null, input.operation);
        } 
    }    
};

var getDataType = function(columnName) {
    var datatype = 'string';
    var chops = columnName.split('(');
    var fieldName = chops[0];
    if (chops.length > 1) 
        datatype = chops[1].split(')')[0];

    return {
        datatype: datatype,
        fieldName: fieldName
    }
}

var logFields = function(pre, sourceRecord, fieldNames) {
    var s = pre + sourceRecord._id;
    _.each(fieldNames, function(field){
        s += ',"' + field.fieldName + '::' + jsonpath(sourceRecord, field.fieldName) + '"'; 
    });

    h.log('info', s);
}

var processRecord = function (searchBy, searchByValue, fieldNames, fieldValues, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "" + (input.operation) + ' ' + searchBy.fieldName + " on record '" + searchByValue + "': " + JSON.stringify(err));
        } else { 
            h.log('info', "" + (input.operation) + ' ' + searchBy.fieldName + " on record '" + searchByValue + "': with " + fieldValues);
        }
        callback();     
    };

    // Passing  fieldName[0] i.e. searchBy and fieldValue[0] i.e. 
    findSource(searchBy.fieldName, searchByValue, function(err, res) {
        if (err || !res) return done(err);

        async.eachLimit(res, 1, function(sourceRecord, cb) {
	    logFields('EXISTING: ', sourceRecord, fieldNames);
            _.each(fieldNames, function(field, i) {
                if ((fieldValues[i] != '"' && fieldValues[i] != '') || input.updBlank) { 
                    updateAttribute(sourceRecord, field.fieldName, field.datatype, fieldValues[i], cb);
                }
            });
	    logFields('REPLACEM: ', sourceRecord, fieldNames);
            if (input.operation == 'update' || input.operation == 'delete') { 
                    sourceCollection.update(sourceRecord, cb);
            }
        }, 
        done);
    });
};

h.log('info', 'Processing ' + input.file);

h.initLookups(restApi, input.source, function(err) {
    csvHelper.readAsObj(input.file, function (data) {
        async.eachLimit(data, input.limit, function (csvRecord, callback) {
            if (!data) return callback();

            if (!allColumnNames) {
                allColumnNames = _.keys(csvRecord);     // contains column names and datatype
                globalSearchBy = getDataType(_.first(allColumnNames));    //  Identify searchBy(first column) from all
                globalFields = _.map(_.rest(allColumnNames, 1), getDataType  );  // building Object  of string and field Name
            }

            var fieldValues = _.rest(_.values(csvRecord), 1);
            var searchByValue = _.first(_.values(csvRecord));   

           // Passing fieldNames, datatype and values in Array
            if (searchByValue && (input.operation == 'delete' || input.operation == 'update')) {
                processRecord(globalSearchBy, searchByValue, globalFields, fieldValues, callback);
            } else {
                h.log('warn', 'Skipping ' + csvRecord);
                callback();
            }
        },
        function (err) {
            h.log('info', 'DONE ' + err);
        });
    });
});
