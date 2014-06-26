#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var moment = require("moment");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility resolves opportunity as a closed in Renew, using a CSV input.\
        \nThe input file should contain these columns, "Name" with the opportunity displayName and "Reason" code.\
        \noppName, resolutionDate, poAmount, poDate, poNumber, reason, soAmount, soDate, soNumber\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', '_id')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 3)
    .alias('z', 'zenMode').describe('z', 'Zen mode handles most exceptions and forces opp close').default('z', false)
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    bookingCollection = h.getCollection(restApi, "app.bookings"),
    itemsCollection = h.getCollection(restApi, "app.lineitems"),
    lookupCollection = h.getCollection(restApi, "app.lookups"),
    csvHelper = new csvHelperInstance();

var findOpportunity = function (value, callback) {
    h.findRecords(oppCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var findReason = function (value, callback) {
    h.findCachedRecords(lookupCollection, {
        filter: {name: value},
    }, callback);
};

var resolveAsWin = function (opportunity, params, callback) {
    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        },
        selections: []
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'getCompleteBookingInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.opp.complete.booking.input'] || !res.data['app.opp.complete.booking.input'][0]) 
            return callback(err || res);

        var input = res.data['app.opp.complete.booking.input'][0];
        input.request.requestReason = {name: 'initialQuote'};

        if (params.resolutionDate) {
            var r = h.noonOffset(params.resolutionDate);

            input.create.creationDate = r;
            input.create.expirationDate = r;
            input.request.dueDate = r;
            input.send.deliveryDate = r;
        }

        if (!_.isEmpty(params.poAmount)) input.completeBooking.poAmount = {amount: params.poAmount}; 
        if (!_.isEmpty(params.soAmount)) input.completeBooking.soAmount = {amount: params.soAmount}; 

        if (params.poDate) input.completeBooking.poDate = h.noonOffset(params.poDate);
        if (params.poNumber) input.completeBooking.poNumber = params.poNumber;
        if (params.soDate) input.completeBooking.soDate = h.noonOffset(params.soDate);
        if (params.soNumber) input.completeBooking.soNumber = params.soNumber;
        if (params.resultReason) input.completeBooking.winResultReason = {
            name: params.resultReason.name,
            displayName: params.resultReason.displayName
        };
        input.completeBooking.lossResultReason = {
            name: 'haBDT'
        };

        tenantApi.execute('app.opportunities', opportunity._id, 'completeBooking', input, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['app.booking/sales'] || !res.data['app.booking/sales'][0]) 
                return callback(err || res);

            callback(err, res.data['app.booking/sales'][0]);
        }); 
    });
};

var updateBooking = function(booking, params, callback) {
    
    h.findRecords(bookingCollection, {
        filter: {_id: booking.key},
    }, function(err, res) {
        if (err || !res || !res[0]) return callback('Unable to find the booking ' + (booking && booking.key));
        
        var booking = res && res[0];
        if (params.poAmount) booking.poAmount.amount = params.poAmount;
        if (params.soAmount) booking.soAmount.amount = params.soAmount;
        if (params.soDate) booking.soDate = params.soDate;
        if (params.poDate) booking.poDate = params.poDate;
        if (params.poNumber) booking.poNumber = params.poNumber;
        if (params.soNumber) booking.soNumber = params.soNumber;

        bookingCollection.update(booking, function(err, booking) {
            if (err || !booking) return callback('Unable to update the booking ' + (booking && booking._id));

            var payload = {detail: booking};
            tenantApi.execute('app.bookings', booking._id, 'complete', payload, function(err, res) {
                if (err || !res || !res.success || !res.data || !res.data['app.booking/sales'] || !res.data['app.booking/sales'][0]) 
                    return callback(err || res);

                callback(err, res.data['app.booking/sales'][0]);                
            });
        });

    });
};

var adamantFunction = function(opportunity, booking, params, callback) {
    if (h.getFlowState(opportunity, 'salesStages') == 'closedSale') {
        h.log('debug','Opportunity already closed ' + opportunity._id);
        return callback();
    }

    var determineNextSteps = function(err, res) {
        //Get out if not in zen mode or if we max out on 2 retries
        if (!err || !input.zenMode || ++params.tries > 4) return callback(err, res);
   
        //Check and handle transition exception
        if (_.isArray(err) && err[0].message && err[0].message.key == 'metadata.models.app.booking.messages.mustTransitionOpp'
            && err[0].message.params && err[0].message.params[0] && err[0].message.params[0].period) {
            
            h.log('debug', opportunity._id + ' number ' + params.tries + ' attempting selling period update before continuing ');
           if (!opportunity.extensions.master.targetPeriod) 
                opportunity.extensions.master.targetPeriod = {};
 
            opportunity.extensions.master.targetPeriod.value = {name: err[0].message.params[0].period.toLowerCase()};
            oppCollection.update(opportunity, function(err, opp) {
                if(!booking) {
                   var bookings = h.getRels(opportunity, 'booking');
                   if (!_.isEmpty(bookings)) booking = _.find(bookings, function(b) { return getFlowState(b, 'bookingStages') != 'canceled'});
                }
                adamantFunction(opportunity, booking, params, callback);
            });
        }
        else if (_.isArray(err) && err[0].message && err[0].message.key == 'metadata.models.app.quote.messages.cnrbActiveBooking') {
            
            h.log('debug', opportunity._id + ' number ' + params.tries + ' attempting update before continuing for a cnrbActiveBooking');            
            findOpportunity(opportunity._id, function(err, opps) {
                if(!booking) {
                   var bookings = h.getRels(opps && opps[0], 'booking');
                   if (!_.isEmpty(bookings)) booking = _.find(bookings, function(b) { return getFlowState(b, 'bookingStages') != 'canceled'});
                }
                adamantFunction(opps && opps[0], booking, params, callback);
            });
        }

        else if (_.isArray(err) && err[0].message && err[0].message.key == 'metadata.models.app.quote.messages.cnCompleteBooking') {
            
            h.log('debug', opportunity._id + ' number ' + params.tries + ' attempting update before continuing for a cnCompleteBooking'); 
            findOpportunity(opportunity._id, function(err, opps) {
                if(!booking) {
                   var bookings = h.getRels(opps && opps[0], 'booking');
                   if (!_.isEmpty(bookings)) booking = _.find(bookings, function(b) { return getFlowState(b, 'bookingStages') != 'canceled'});
                }
                adamantFunction(opps && opps[0], booking, params, callback);
            });
        }

        // Servers are busy
        else if ((err.code && err.code == 'ECONNRESET')) {
            h.log('warn', "Retry " + opportunity._id + ' number ' + params.tries + ' waiting a bit before moving ahead ');
            return callback(err, res);
            _.delay(function() {
                findOpportunity(opportunity._id, function(err, opps) {
                if(!booking) {
                   var bookings = h.getRels(opportunity, 'booking');
                   if (!_.isEmpty(bookings)) booking = _.find(bookings, function(b) { return getFlowState(b, 'bookingStages') != 'canceled'});
                }
                    adamantFunction(opps && opps[0], null, params, callback);
                });
            },  2 * 60 * 1000); // wait for 2 minutes
        }

        // Servers may be busy or buggy
        else if (_.isArray(err.messages) && err.messages[0].message && err.messages[0].message.key == 'static.server.js.messages.methodTimeout') {
            h.log('warn', "Retry " + opportunity._id + ' number ' + params.tries + ' moving on after a timeout ');
            return callback(err.messages[0].bgJob._id + '::' + err.messages[0].bgJob.name, res);
            /*_.delay(function() {
                findOpportunity(opportunity._id, function(err, opps) {
                if(!booking) {
                   var bookings = h.getRels(opportunity, 'booking');
                   if (!_.isEmpty(bookings)) booking = _.find(bookings, function(b) { return getFlowState(b, 'bookingStages') != 'canceled'});
                }
                    adamantFunction(opps && opps[0], null, params, callback);
                });
            },  2 * 60 * 1000); // wait for 2 minutes
            */
        }
     
        //Unhandled exception
        else {
            h.log('error' , 'Unhandled scenario ' + JSON.stringify(err));
            return callback(err, res);
        }
    };

    if(!booking) {
        var bookings = h.getRels(opportunity, 'booking');
        if (!_.isEmpty(bookings)) booking = _.find(bookings, function(b) { return getFlowState(b, 'bookingStages') != 'canceled'});
    }

    if (!booking) {
        resolveAsWin(opportunity, params, determineNextSteps);
    } else {
        updateBooking(booking, params, determineNextSteps);
    }

};

var processRecord = function (oppName, params, callback) {
    var done = function(err) {
        if (err) {
            h.log('error', "Resolving opportunity '" + oppName + "': " + JSON.stringify(err));
        } else {
            h.log('info', "Resolving opportunity '" + oppName + "' with " + params.reason);
        }

        callback();
    };

    findReason(params.reason, function(err, resultReason) {
        if (err) return done(err);

        params.resultReason = resultReason;
        findOpportunity(oppName, function(err, res) {
            if (err) return done(err);

            async.eachLimit(res, 1, function(opportunity, ocb) {
                adamantFunction(opportunity, null, params, ocb);
            },
            done);
        });
    });
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        var oppName = csvRecord["Name"];

        var params = {
            resolutionDate: csvRecord["resolutionDate"],
            poAmount: csvRecord["poAmount"],
            poDate: h.getCSVDate(csvRecord, 'poDate'),
            poNumber: csvRecord["poNumber"],
            reason: csvRecord["reason"],
            soAmount: csvRecord["soAmount"],
            soDate: h.getCSVDate(csvRecord, 'soDate'),
            soNumber: csvRecord["soNumber"],
            tries: 0,
        };

        if (oppName && !_.isEmpty(params)) {
            processRecord(oppName, params, callback);
        } else {
            h.log('warn', 'Skipping ' + oppName);
            callback();
        }
    },
    function (err) {
        h.log('info', 'DONE ' + err);
    });
});

