#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");
var moment = require('moment');

var input = require('optimist')
    .usage('\nREADME: This is a utility to add or remove attributes for any object in Renew, using a CSV input.\
        \nThe input file should contain two columns, "Name" column with the source object displayName and "Field Name"\
        \nSample CSV File format contains _id and amount(amount.amount) fields\
        \n==========================================\
        \nName,Field Name\
        \n52d92e8c36d0cfe25f0119a5,50.0\
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
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', '_id')
    .alias('e', 'field').describe('e', 'Field to modify.').default('e', 'displayName')
    .alias('c', 'schemeName').describe('c', 'Name for scheme Id Ex. Dataload, Batchload').default('c','')
    .alias('d', 'datatype').describe('d', 'Data Type of the value being set ["boolean", "date", "string", "number"]').default('d', "string")
    .alias('o', 'operation').describe('o', 'Operation to perform [update, delete]').default('o', 'update')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    sourceCollection = h.getCollection(restApi, input.source),
	csvHelper = new csvHelperInstance();

var findSource = function (value, callback) {
    h.findRecords(sourceCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var updateAttribute = function (sourceRecord, targetValue, callback) {

    if (input.operation == 'update') {

        // Check format and set appropriate type
        if (input.datatype == 'number') {
            targetValue = parseFloat(targetValue);
            if (!_.isNumber(targetValue)) return callback();
        }

        if (input.datatype == 'boolean') {
            targetValue = (targetValue === 'true');
            if (!_.isBoolean(targetValue)) return callback();
        }

        if (input.datatype == 'date') {
            targetValue = h.noonOffset(targetValue);
        }

        if (input.field == 'tags') {
            sourceRecord.tags.push(targetValue);
        } else if (input.field == 'externalIds.id') {
              if (input.schemeName) {
                   var found = 0;
                   var foundmatch = 0;
                   _.each(sourceRecord.externalIds, function(scheme) {
                       if (scheme.schemeId && scheme.schemeId.name == input.schemeName) {
                        	sourceRecord.externalIds[found].id = targetValue;
                            foundmatch++;
                       }
                      found++;
                    });
                    if (foundmatch == 0) {
                        sourceRecord.externalIds.push({id: targetValue, schemeId: {name: input.schemeName}});
                    }
              } else {
                   sourceRecord.externalIds.push({id: targetValue, schemeId: {name: 'externalId'}});
              }
    	} else if (! h.startsWith(input.field, 'flows')) {

        /*   else if (input.field.indexOf('flows') == 0) {
    		var flowName = input.field.split('.')[1];
    		var targetState = targetValue;
    		var result = targetValue.replace( /([A-Z])/g, " $1" );
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

            h.deepSet(sourceRecord, input.field, targetValue, input.operation);
    	}
    } else if (input.operation == 'delete') {
        h.deepSet(sourceRecord, input.field, null, input.operation);
    }

	sourceCollection.update(sourceRecord, callback);
};

var processRecord = function (sourceName, targetValue, callback) {

    findSource(sourceName, function(err, res) {
        if (err || !res) return callback(err);

        async.eachLimit(res, 1, function(sourceRecord, cb) {

            if (input.operation == 'update' || input.operation == 'delete') {
                updateAttribute(sourceRecord, targetValue, function(err, updatedRecord) {

                    if (err) {
                        h.log('error', "" + (input.operation) + ' ' + input.field + " on record '" + sourceName + "': " + JSON.stringify(err));
                    } else {
                        h.log('info', "PROCESSED :" + sourceName + ',' + h.getObjectValueFromPath(updatedRecord, input.field));
                    }
                    cb();
                });
            } else if (input.operation == 'log')  {
                h.log('info', "LOG :" + sourceName + ',' + h.getObjectValueFromPath(sourceRecord, input.field));
                cb();
            }
        },
        callback);
    });
};

if (input.schemeName && (input.field !== 'externalIds.id')){
      h.log('error', "Cannot update schemeId, field name should be externalIds.id") ;
}
else {
      h.log('info', 'Processing ' + input.file);
      csvHelper.readAsObj(input.file, function (data) {
      async.eachLimit(data, input.limit, function (csvRecord, callback) {
        	if (!data) return callback();

        	var sourceName = csvRecord["Name"];
        	var targetValue = csvRecord["Field Name"];

        	if (sourceName && (input.operation == 'delete' || input.operation == 'log' || input.operation == 'update' )) {
            	processRecord(sourceName, targetValue, callback);
        	} else {
            	h.log('warn', 'Skipping ' + sourceName + ' and ' + targetValue);
            	callback();
        	}
    	 },
    	  function (err) {
        	h.log('info', 'DONE ' + err);
    	  });
     });
};
