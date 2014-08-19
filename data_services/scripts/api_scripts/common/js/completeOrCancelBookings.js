#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility that cancels bookings.\
        \nThe input file should contain two columns, "_id" for the booking and "cancelReason".\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', '_id')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('y', 'usingOpportunity').describe('y', 'using opportunity instead of booking based search, cancels all open bookings').boolean('y').default('y', false)
    .alias('o', 'operation').describe('o', 'cancel or complete the open bookings').default('o', 'complete')
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    lookupCollection = h.getCollection(restApi, "app.lookups"),
    bookingCollection = h.getCollection(restApi, "app.bookings"),
    csvHelper = new csvHelperInstance();

var findOpportunityBookings = function (value, callback) {
    var filter = {};
    filter[input.searchBy] = value;

    h.findRecords(oppCollection, {
        multiple: input.multiple,
        filter: filter,
    }, function(err, opps) {
        if (err || !opps) return callback(err);

        var bIds = h.getRelKeys(opps[0], 'booking');
        var filter = {
            _id: {$in: bIds},
            'flows.bookingStages.state.name': {$ne: 'canceled'}
        };

        h.findRecords(bookingCollection, {
            multiple: true,
            filter: filter,
        }, callback);   
    });
};

var findBooking = function (value, callback) {
    var filter = {
        'flows.bookingStages.state.name': {$ne: 'canceled'}        
    };
    filter[input.searchBy] = value;

    h.findRecords(bookingCollection, {
        multiple: input.multiple,
        filter: filter,
    }, callback);
};

var cancelBooking = function (booking, reason, callback) {
    var payload = {
        detail: {
            _id: booking._id,
            displayName: booking.displayName,
            type: booking.type
        }
    };

    tenantApi.execute('app.bookings', booking._id, 'getCancelInput', payload, function(err, res) {
        if (err || !res || res.success != true || !res.data || !res.data['app.cancel.booking.input'] || !res.data['app.cancel.booking.input'][0])
            return callback("on getCancelInput " + JSON.stringify(err || res));

        var payload2 = res.data['app.cancel.booking.input'][0];
        payload2.cancellationReason = {name: reason};
        tenantApi.execute('app.bookings', booking._id, 'cancel', payload2, function(err, res) {
            var book = res && res.data && res.data['app.booking/sales'] && res.data['app.booking/sales'][0];

            if (err) {
                h.log('error', "Canceling bookings '" + booking._id + "': " + JSON.stringify(err));
            } else {
                h.log('info', "Canceling bookings '" + booking._id + ' stage is now ' + h.getFlowState(book, 'bookingStages'));
            }
            return callback(err, res && res.data);
        });
    });
};

var completeBooking = function (booking, callback) {
    var payload = {
        detail: {
            _id: booking._id,
            displayName: booking.displayName,
            type: booking.type
        }
    };

    tenantApi.execute('app.bookings', booking._id, 'complete', payload, function(err, res) {
        var book = res && res.data && res.data['app.booking/sales'] && res.data['app.booking/sales'][0];

        if (err) {
            h.log('error', "Completing bookings '" + booking._id + "': " + JSON.stringify(err));
        } else {
            h.log('info', "Completing bookings '" + booking._id + ' stage is now ' + h.getFlowState(book, 'bookingStages'));
        }

        return callback(err, res && res.data);
    });
};

var processRecord = function (bookingName, reason, callback) {
    var done = function(err) {
        h.log('error', err);
        callback();
    };
    var p = function(err, res) {
            if (err) return done(err);

            async.eachLimit(res, 1, function(booking, ocb) {
                if (input.operation == 'cancel') 
                    cancelBooking(booking, reason, ocb);
                else 
                    completeBooking(booking, ocb);
            },
            done);
        };
    
    if (input.usingOpportunity) {
        findOpportunityBookings(bookingName, p);
    } else {
        findBooking(bookingName, p);
    }
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();
        var bookingName = csvRecord["Name"];
        var cancelReason = (_.isEmpty(csvRecord["Reason"]) ? 'userErrorBookingData' :  csvRecord["Reason"]);

        if (bookingName && cancelReason) {
            processRecord(bookingName, cancelReason, callback);
        } else {
            h.log('warn', 'Skipping ' + bookingName );
            callback();
        }
    },
    function (err) {
        h.log('info', 'DONE ' + err);
    });
});
