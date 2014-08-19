#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility splits an opportunity, using a CSV input.\
        \nThe input file should contain two columns, "Opportunity" with the opportunity identifier and "OfferIds", an array of offer _ids comma separated.\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName]').default('b', '_id')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    csvHelper = new csvHelperInstance();

var findOpportunity = function (value, callback) {
    h.findRecords(oppCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var cancelBooking = function (booking, reason, oppName, callback) {
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
                h.log('warn', 'Reverting opportunity ' + oppName + " issue when canceling booking '" + booking._id + "': " + JSON.stringify(err));
            } else {
                h.log('debug', 'Reverting opportunity ' + oppName + " completed cancel booking '" + booking._id);
            }
            return callback(err, res && res.data);
        });
    });
};

var revertStage = function (opportunity, reason, callback) {
    if (!opportunity) return callback('No Opp ' + opportunity);

    // If bookings exist, cancel them; if not, revert the opportunity
    var bookings = h.getRels(opportunity, 'booking');
    if (!_.isEmpty(bookings) && _.find(bookings, function(book){ return h.getFlowState(book, 'booking') != 'canceled'})) {
        var booking = _.find(bookings, function(book) { return h.getFlowState(book, 'booking') != 'canceled'});
        booking._id = booking.key;
        cancelBooking(booking, reason, opportunity._id, callback);
    } else {
        tenantApi.execute('app.opportunities', opportunity._id, 'resetToQuoteDelivered', {}, function(err, res) {
            
            if (err || res.success != true || !res)  {
                h.print('FAIL|', [opportunity._id]);
                return callback("on revertStage " + JSON.stringify(err));
            }
            callback(null, res);
        });        
    }
};

var processRecord = function (oppName, reason, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "Reverting opportunity '" + oppName + "': " + JSON.stringify(err));
            h.print('FAIL|', [oppName]);
        } else {
            h.log('info', "Reverting opportunity '" + oppName + "' with " + reason);
        }
        callback();
    };

    findOpportunity(oppName, function(err, opps) {
        if (err) return done(err);

        async.eachLimit(opps, 1, function(opportunity, ocb) {
            if (h.getFlowState(opportunity, 'salesStages') == 'quoteDelivered') {
                h.log('debug', 'Opportunity already in quote delivered status ' + opportunity._id);
                return ocb();
            }
        
            var partnerOpp = h.getRel(opportunity, 'subordinateOpportunity');
            if (partnerOpp) {
                findOpportunity(partnerOpp.key, function(err, popps) {
                    if (err) return callback(err);

                    revertStage(popps[0], reason, ocb); 
                });
            } else {
                revertStage(opportunity, reason, ocb);                 
            }
        },
        done);
    });
};

h.log('info', 'Processing ' + input.file);
h.print('FAIL|', ['Opportunity', 'Reason']); // for auto re-processing

csvHelper.readAsObj(input.file, function (data) {
    if (!data) return callback();
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        var oppName = csvRecord["_id"] || csvRecord["Opportunity"] || csvRecord["Name"];
        var cancelReason = (_.isEmpty(csvRecord["Reason"]) ? 'userErrorBookingData' :  csvRecord["Reason"]);

        if (oppName) {
            processRecord(oppName, cancelReason, callback);
        } else {
            h.log('warn', 'Skipping ' + oppName + ' ' + cancelReason);
            callback();
        }
    },
    function (err) {
        h.log('info', "DONE " + err);
    });
});
