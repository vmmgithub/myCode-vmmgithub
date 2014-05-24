#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility that attempts to complete, using a CSV input.\
        \nThe input file should contain two columns, "Name".\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', '_id')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    bookCollection = h.getCollection(restApi, "app.bookings"),
    csvHelper = new csvHelperInstance();

var findBooking = function (value, callback) {
    h.findRecords(bookCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var complete = function (booking, callback) {
    if (h.getFlowState(booking, 'bookingStages') != 'pending') return callback();

    var payload = {
        detail: {
            _id: booking._id,
            displayName: booking.displayName,
            type: booking.type
        },
    };

    tenantApi.execute('app.bookings', booking._id, 'complete', payload, function(err, res) {
        if (err || !res || res.success != true || !res.data)
            return callback("on complete " + JSON.stringify(err || res));

        return callback(null, res);
    });
};

var processRecord = function (bookingName, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "Completing booking '" + bookingName + "': " + JSON.stringify(err));
        } else {
            h.log('info', "Completing booking '" + bookingName+ "'");
        }

        callback();
    };

    findBooking(bookingName, function(err, res) {
        if (err) return done(err);

        async.eachLimit(res, 1, complete, done); 
    });
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();
            var bookingName = csvRecord["Name"];

            if (bookingName) {
                processRecord(bookingName, callback);
            } else {
                h.log('warn', 'Skipping ' + bookingName);
                callback();
            }
        },
        function (err) {
            h.log('info', 'DONE ' + err);
        });
});

