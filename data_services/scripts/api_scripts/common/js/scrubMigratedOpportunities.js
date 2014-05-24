#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var moment = require('moment');
var csvHelperInstance = require('../../lib/helpers/CsvHelper');

var input = require('optimist')
    .usage('\nREADME: This is a utility used for scrubbing opportunities in Renew, based on Atlas values.\
        \n\nThe input file should contain these columns: ...... : \
        \n opportunityid,targetPeriod,ssisalesstage,SalesRep,resolutiondate,localtransactionamount,commitlevel,firstquotedate,firstcontactdate,newdistributorid,newresellerid,newponumber,ssibookingdate,newsonumber,clientbookingdate,ssiresultreason,ssiquotenumberid,forecastedclosedate\
        \n "8C798CA0-047D-E111-80DD-0017A4770430","fy12q2","Closed Sale","7246C643-F040-E211-AFC0-0025B5C1AA9C","2012-10-09 04:00:00.000","71658.8000000000","Green","2012-04-02 13:44:26.000","2012-04-02 13:44:26.000","219EDE41-A4F9-DE11-9A71-0017A4770430","B2BF8F20-64A2-E011-BC6A-0017A4770430","4500035504OD","2012-10-08 00:00:00.000","150886","2012-10-08 00:00:00.000","CS - Co-term Short - CTS","5946CCD6-835E-4C26-BC2B-48FD0D17443B","2012-10-09 04:00:00.000"\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port')
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password')
    .alias('f', 'file').describe('f', 'File to process')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', true)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('k', 'skip').describe('k', 'Number of lines to skip in the input file').default('k', 0)
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalIds.id]').default('b', 'extensions.tenant.atlasMigrationId.value')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    quoteCollection = h.getCollection(restApi, 'app.quotes'),
    bookingCollection = h.getCollection(restApi, 'app.bookings'),
    contactsCollection = h.getCollection(restApi, 'core.contacts'),
    coreLookupCollection = h.getCollection(restApi, 'core.lookups'),
    appLookupCollection = h.getCollection(restApi, 'app.lookups'),
    csvHelper = new csvHelperInstance();

// -----------------------------------------------------------
// Cache Functions
// -----------------------------------------------------------
var cache = {};
var loadLookups = function(group, type, callback) {
    if (!callback) return;
    if (!group || !type) return callback('Nothing to lookup');
    if (cache[type] && cache[type][group]) return callback(null, cache[type][group]);

    var coll = (type == 'core')? coreLookupCollection : appLookupCollection;
    coll.find({group: group}, {params: {limit: 500}}, function(err, records) {
        if (err || !records || records.length == 0)
            return callback(err || 'No lookup records found');

        if (!cache[type]) cache[type] = {};
        if (!cache[type][group]) cache[type][group] = [];

        _.each(records, function(lkp) {
            cache[type][group].push({
                type: lkp.type,
                key: lkp._id,
                displayName: lkp.displayName,
                name: lkp.name,
                value: lkp.value,
            });
        });

        return callback(null, cache[type][group]);
    });
}

var getLookup = function(group, type, name) {
    var retVal;
    var n = '' + name;
    n = n.replace(/[^a-z0-9\s]/gi, '').toLowerCase();

    if (cache[type] && cache[type][group]) {
        _.each(cache[type][group], function(l) {
            if (l.displayName == name || l.name == name
                || l.displayName.toLowerCase().replace(/[^a-z0-9\s]/gi, '') == n
                || l.name.toLowerCase().replace(/[^a-z0-9\s]/gi, '') == n)
                retVal = l;
        });
    }
    return retVal;
}

var init = function(callback) {
    async.parallel([
        function(cb) {
            loadLookups('TargetSelling', 'app', cb);
        },
        function(cb) {
            loadLookups('CommitLevel', 'app', cb);
        },
        function(cb) {
            loadLookups('ResultReason', 'app', cb);
        }
    ],
    callback);
};

var findOpportunity = function (value, callback) {
    h.findRecords(oppCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var findBooking = function (filter, callback) {
    h.findRecords(bookingCollection, {
        multiple: input.multiple,
        filter: filter,
    }, callback);
};

var checkFlowTransition = function(opportunity, state) {
    return _.find(opportunity.flows.salesStages.transitions, function(t){ return t.toState == state});
};

var getOppId = function(opportunity) {
    return opportunity.extensions.tenant && opportunity.extensions.tenant.opportunityatlasid && opportunity.extensions.tenant.opportunityatlasid.value;
};

var findSalesRep = function (xid, callback) {
    if (!xid) return callback();
    var filter = {};
    filter['externalIds.id'] = {'$regex': '^' + xid};

    h.findCachedRecords(contactsCollection, {
//        filter: filter,
        searchBy : 'externalIds.id',
        value : xid
    }, function(err, records) {
        return callback(null, h.getTargetPointer(records));
    });
};

var updateBaseAttributes = function (opportunity, resolutionDate, commitLevel, callback) {
    var tsp;
    var modified = false;
    if (resolutionDate) {
        resolutionDate = moment(resolutionDate);
        tsp = _.find(cache['app']['TargetSelling'], function(sp) {
            return ((resolutionDate.isAfter(sp.value.start) || resolutionDate.isSame(sp.value.start))
                && (resolutionDate.isBefore(sp.value.end) || resolutionDate.isSame(sp.value.end)));
        });

        if(tsp && (!opportunity.extensions.master.targetPeriod ||opportunity.extensions.master.targetPeriod.value.name != tsp.name)) {
            opportunity.extensions.master.targetPeriod = {type: 'lookup', value: tsp};
            modified = true;
        }
    }

    if(commitLevel) {
        commitLevel = getLookup('CommitLevel', 'app', commitLevel);

        if((commitLevel && !opportunity.commitLevel) || (commitLevel && opportunity.commitLevel.name != commitLevel.name)) {
            opportunity.commitLevel = commitLevel;
            modified = true;
        }
    }

    if (modified) {
        h.log('debug', '[' + getOppId(opportunity) + '] Updating baseline properties');
        oppCollection.update(opportunity, callback);
    }
    else callback();
};

var assignSalesRep = function (opportunity, salesRepId, callback) {
    if (_.isEmpty(salesRepId)) return callback(null, opportunity);

    oppCollection.find({_id: opportunity._id}, {}, function(err, records) {
        if (err || !records || records.length == 0)
            return callback(err || 'No source records found');

        opportunity = records[0];

        findSalesRep(salesRepId, function(err, salesRep) {

            if (!salesRep) return callback( err || 'No salesRep record found =>' + salesRepId);

            var existingRep = h.getRel(opportunity, 'salesRep');
            if (existingRep && existingRep.key == salesRep.key) return callback();

            opportunity.relationships.push({relation: {name: 'salesRep'}, target: salesRep});
            h.log('debug', '[' + getOppId(opportunity) + '] Updating salesRep');
            oppCollection.update(opportunity, callback);
        });
    });
};

var logCustomerContact = function (opportunity, contactedDate, callback) {
    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        },
        selections: []
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'getCustomerContactInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.customer.contact.input'] || !res.data['app.customer.contact.input'][0])
            return callback('on getCustomerContactInput ' + JSON.stringify(err));

        var input = res.data['app.customer.contact.input'][0];

        input.contactDate = contactedDate;
        input.successful = true;
        input.notes.push({type: 'core.note', text: 'Automated customer contact'})

        tenantApi.execute('app.opportunities', opportunity._id, 'logCustomerContact', input, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['app.opportunity'] || !res.data['app.opportunity'][0])
                return callback('on logCustomerContact ' + JSON.stringify(err || res));

            return callback(null, res.data['app.opportunity'][0]);
        });
    });
};

var addQuoteId = function(quote, quoteId, callback) {
    if (!quote || !quoteId) callback(null, quote);

    if (!_.isArray(quote.externalIds)) quote.externalIds = [];
    quote.externalIds.push({id: quoteId, schemeId: {name: 'atlasMigrationId'}});

    var found = 0;
    _.each(quote.externalIds, function(scheme) {
        if (scheme.schemeId && scheme.schemeId.name == 'quoting') {
            quote.externalIds[found].id = '';
        }
        found++;
    });

    quoteCollection.update(quote, callback);
};

var requestQuote = function (opportunity, quotedDate, quoteId, callback) {
    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        },
        selections: []
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'getQuoteInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.quote.input'] || !res.data['app.quote.input'][0])
            return callback("on getQuoteInput " + JSON.stringify(err || res));

        var input = res.data['app.quote.input'][0];
        input.resultReason = {name: 'initialQuote'}; //Hard coded reason
        input.notes.push({type: 'core.note', text: 'Automated request quote'})

        if (quotedDate) input.dueDate = moment(quotedDate);

        tenantApi.execute('app.opportunities', opportunity._id, 'requestQuote', input, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['app.quote'] || !res.data['app.quote'][0])
                return callback("on requestQuote " + JSON.stringify(err || res));

            addQuoteId(res.data['app.quote'][0], quoteId, callback);
        });
    });
};

var completeQuote = function (opportunity, quotedDate, quoteId, callback) {
    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        },
        selections: []
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'getCompleteQuoteInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.opp.complete.quote.input'] || !res.data['app.opp.complete.quote.input'][0])
            return callback("on getCreateQuoteInput " + JSON.stringify(err || res));

        var input = res.data['app.opp.complete.quote.input'][0];
        input.request.requestReason = {name: 'initialQuote'}; //Hard coded reason
        input.notes.push({type: 'core.note', text: 'Automated complete quote'})
        if (quotedDate) {
            input.create.creationDate = moment(quotedDate);
            input.create.expirationDate = moment(quotedDate);
        }

        tenantApi.execute('app.opportunities', opportunity._id, 'completeQuote', input, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['app.quote'] || !res.data['app.quote'][0])
                return callback("on completeQuote " + JSON.stringify(err || res));

            addQuoteId(res.data['app.quote'][0], quoteId, callback);
        });
    });
};

var deliverQuote = function (opportunity, quote, quotedDate, callback) {
    var payload = {
        detail: {
            _id: quote._id,
            displayName: quote.displayName,
            type: quote.type
        },
        selections: []
    };

    tenantApi.execute('app.quotes', quote._id, 'getSendQuoteInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.send.quote.input'] || !res.data['app.send.quote.input'][0])
            return callback("on getSendQuoteInput " + JSON.stringify(err || res));

        var input = res.data['app.send.quote.input'][0];
        input.notes.push({type: 'core.note', text: 'Automated deliver quote'})
        if (quotedDate) input.deliveryDate = moment(quotedDate);

        tenantApi.execute('app.quotes', quote._id, 'sendQuote', input, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['app.quote'] || !res.data['app.quote'][0])
                return callback("on sendQuote " + JSON.stringify(err || res));

            callback(err, res.data['app.quote'][0]);
        });
    });
};

var commitQuote = function (opportunity, quote, quotedDate, forecastedCloseDate, callback) {
    var payload = {
        detail: {
            _id: quote._id,
            displayName: quote.displayName,
            type: quote.type
        },
        selections: []
    };

    tenantApi.execute('app.quotes', quote._id, 'getCustomerCommitmentInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.customer.commitment.input'] || !res.data['app.customer.commitment.input'][0])
            return callback("on getCustomerCommitmentInput " + JSON.stringify(err || res));

        var input = res.data['app.customer.commitment.input'][0];
        if (quotedDate) input.contactDate = moment(quotedDate);
        if (forecastedCloseDate) input.forecastedCloseDate = moment(forecastedCloseDate);

        tenantApi.execute('app.quotes', quote._id, 'setCustomerCommitment', input, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['app.opportunity'] || !res.data['app.opportunity'][0])
                return callback("on setCustomerCommitment " + JSON.stringify(err || res));

            callback(err, res.data['app.opportunity'][0]);
        });
    });
};

var poReceived = function (opportunity, quotedDate, quoteId, poAmount, poDate, poNumber, resultReason, callback) {
    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        },
        selections: []
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'getRequestBookingInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.opp.request.booking.input'] || !res.data['app.opp.request.booking.input'][0])
            return callback("on getRequestBookingInput " + JSON.stringify(err || res));

        var input = res.data['app.opp.request.booking.input'][0];
        if (quotedDate) {
            input.create.creationDate = moment(quotedDate);
            input.create.expirationDate = moment(quotedDate);
            input.request.dueDate = moment(quotedDate);
            input.send.deliveryDate = moment(quotedDate);
        }

        if (!_.isEmpty(poAmount)) input.requestBooking.poAmount = {amount: poAmount};
        input.requestBooking.poDate = moment(poDate);
        input.requestBooking.poNumber = poNumber;
        input.requestBooking.winResultReason = getLookup('ResultReason', 'app', (resultReason || 'CS - Renewed at Par - R@P'));

        tenantApi.execute('app.opportunities', opportunity._id, 'requestBooking', input, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['app.booking/sales'] || !res.data['app.booking/sales'][0])
                return callback("on requestBooking " + JSON.stringify(err || res));

            addQuoteId(res.data['app.quote'][0], quoteId, function(e, r) {
                callback(err || e, res.data['app.booking/sales'][0]);
            });
        });
    });
};

var resolveAsWin = function (opportunity, quoteId, resolutionDate, poAmount, poDate, poNumber, resultReason, soDate, soNumber, callback) {
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
            return callback("on getCompleteBookingInput " + JSON.stringify(err || res));

        var input = res.data['app.opp.complete.booking.input'][0];
        input.request.requestReason = {name: 'initialQuote'};

        if (resolutionDate) {
            var r = moment(resolutionDate);
            r.hour(12);
            input.create.creationDate = r;
            input.create.expirationDate = r;
            input.request.dueDate = r;
            input.send.deliveryDate = r;
        }

        if (!_.isEmpty(poAmount)) {
            input.completeBooking.poAmount = {amount: poAmount};
            input.completeBooking.soAmount = {amount: poAmount};
        }

        input.completeBooking.poDate = moment(poDate).toISOString();
        input.completeBooking.poNumber = poNumber;
        input.completeBooking.soDate = moment(soDate).toISOString();
        input.completeBooking.soNumber = soNumber;
        input.completeBooking.winResultReason = getLookup('ResultReason', 'app', (resultReason || 'CS - Renewed at Par - R@P'));

        tenantApi.execute('app.opportunities', opportunity._id, 'completeBooking', input, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['app.booking/sales'] || !res.data['app.booking/sales'][0])
                return callback("on completeBooking " + JSON.stringify(err || res));

            var opp = res.data['app.opportunity'][0];
            var booking = res.data['app.booking/sales'][0];

            addQuoteId(res.data['app.quote'][0], quoteId, function(e, r) {

                // Time to work around a Renew bug that puts the opp in PO Received state for Channel Tier 2 engagements
                if (opp.flows.salesStages.state.name != 'closedSale') {
                    completeDistiBooking(opp, booking, resolutionDate, poAmount, poDate, poNumber, soDate, soNumber, callback);
                } else {
                    callback(err, res.data['app.booking/sales'][0]);
                }
            });

        });
    });
};

var resolveAsLoss = function (opportunity, resolutionDate, resultReason, callback) {
    if (!resultReason || !getLookup('ResultReason', 'app', resultReason)) return callback();

    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        }
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'getResolveAsLossInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.resolve.loss.input'] || !res.data['app.resolve.loss.input'][0])
            return callback("on loss input " + JSON.stringify(err));

        var input = res.data['app.resolve.loss.input'][0];
        input.resultReason = getLookup('ResultReason', 'app', resultReason);
        input.lossDate = resolutionDate;
        input.notes = [{text: 'Automated resolve as loss', type: 'core.note'}];
        tenantApi.execute('app.opportunities', opportunity._id, 'resolveAsLoss', input, function(err, res) {
            if (err || !res || !res.success)
                return callback(" setting loss input " + JSON.stringify(err || res));

            return callback(null, res && res.data);
        });
    });
};

var completeDistiBooking = function (opportunity, booking, resolutionDate, poAmount, poDate, poNumber, soDate, soNumber, callback) {
    if (!booking) booking = h.getRel(opportunity, 'booking');
    if (!booking) {
        h.log('warn', '"' + opportunity.extensions.tenant.opportunityatlasid.value + '"' + ' Unable to find booking to closeOut opportunity ' + opportunity.displayName);
        return callback();
    }

    findBooking({_id: booking.key || booking._id}, function(err, res) {
        if (err || !res || res.length == 0) {
            h.log('warn', '"' + opportunity.extensions.tenant.opportunityatlasid.value + '"' + ' Unable to find booking to closeOut opportunity ' + opportunity.displayName);
            return callback();
        }

        booking = res[0];
        if (!_.isEmpty(poAmount)) {
            booking.poAmount = {amount: poAmount};
            booking.soAmount = {amount: poAmount};
        }

        booking.poDate = moment(poDate).toISOString();
        booking.poNumber = poNumber;
        booking.soDate = moment(soDate).toISOString();
        booking.soNumber = soNumber;

        bookingCollection.update(booking, function(err, book) {
            tenantApi.execute('app.bookings', booking._id, 'resolveDistributor', booking, function(err, res) {
                if (err || !res || !res.success)
                    return callback("on resolveDistributor " + JSON.stringify(err || res));

                tenantApi.execute('app.bookings', booking._id, 'complete', booking, function(err, res) {
                    if (err || !res || !res.success || !res.data || !res.data['app.opportunity'] || !res.data['app.opportunity'][0])
                        return callback("on complete " + JSON.stringify(err || res));

                    callback(err, res.data['app.opportunity'][0]);
                });
            });
        });
    });
};

var doTheWork = function (csvRecord, rowNum, callback) {
    if (!csvRecord) return callback();

    var oppName;
    var oppId = csvRecord['opportunityid'];
    var quoteId = csvRecord['ssiquotenumberid'];
    var salesStage = csvRecord['ssisalesstage'];
    var commitLevel = csvRecord['commitlevel'];
    var resolutionDate = h.getCSVDate(csvRecord, 'resolutiondate');
    var contactedDate = h.getCSVDate(csvRecord, 'firstcontactdate');
    var quotedDate = h.getCSVDate(csvRecord, 'firstquotedate');
    var poDate = h.getCSVDate(csvRecord, 'ssibookingdate');
    var soDate = h.getCSVDate(csvRecord, 'clientbookingdate');
    var poNumber = csvRecord['newponumber'];
    var poAmount = csvRecord['localtransactionamount'] || 0;
    var soNumber = csvRecord['newsonumber'];
    var resultReason = csvRecord['ssiresultreason'];
    var salesRep = csvRecord['SalesRep'];
    var forecastedCloseDate = h.getCSVDate(csvRecord, 'forecastedclosedate');

    if (resultReason == '<none>') resultReason = null;
    if (salesStage == 'Quote Request') salesStage = 'Quote Requested';
    if (contactedDate && resolutionDate && contactedDate.isAfter(resolutionDate)) contactedDate = resolutionDate;
    if (quotedDate && resolutionDate && quotedDate.isAfter(resolutionDate)) quotedDate = resolutionDate;

    if (!oppId) {
        h.log('info', 'Skipping ' + csvRecord.toString() + ' because of missing data');
        return callback();
    }

    var done = function(err) {
            if (err)
                h.log('error', '' + rowNum + ' Done opportunity "' + oppId + '" : ' + oppName + ' ' + JSON.stringify(err));
            else
                h.log('info', '' + rowNum + ' Done opportunity "' + oppId + '" ' + oppName);

            return callback();
        };

    findOpportunity(oppId, function(err, res) {
        if (err || !res || res.length == 0) return done(err);

        var masterOpps = _.filter(res, function(o) { return !o.isSubordinate; }),
            partnerOpps = _.filter(res, function(o) { return o.isSubordinate; }),
            allOpps = res,
            opps = !_.isEmpty(partnerOpps) ? partnerOpps : (!_.isEmpty(masterOpps) ? masterOpps : []);

        oppName = _.first(res).displayName;

        if (_.isEmpty(masterOpps)) { err="Missing master opportunity "; return done(err)};

        if (_.first(masterOpps).flows.salesStages.state.displayName == salesStage && salesStage != 'Not Contacted') {
            h.log('info', 'Skipping record as the flow state is already in sync');
            return done(null);
        }

        async.series([

            // Step 0: Fix Target Selling Period for all Opps
            function(cb) {
                var innerDone = _.after(allOpps.length, cb);
                _.each(allOpps, function(opp) {
                    updateBaseAttributes(opp, resolutionDate, commitLevel, innerDone);
                });
            },

            // Step 1: Assign Sales Reps
            function(cb) {
                var innerDone = _.after(masterOpps.length, cb);
                _.each(masterOpps, function(opp) {
                    assignSalesRep(opp, salesRep, innerDone);
                });
            },

            //Step 2: Log customer contact, if applicable
            function(cb) {
                if (salesStage == 'Not Contacted' || !contactedDate) return cb();
                var innerDone = _.after(opps.length, cb);

                _.each(opps, function(opp) {
                    if (checkFlowTransition(opp, 'contacted')) return innerDone();

                    h.log('debug', '[' + oppId + '] logging customer contacts');
                    logCustomerContact(opp, contactedDate, innerDone);
                });
            },

            //Step 3: Support Quote Request state
            function(cb) {
                if (salesStage != 'Quote Requested') return cb();
                var innerDone = _.after(opps.length, cb);

                _.each(opps, function(opp) {
                    h.log('debug', '[' + oppId + '] requesting quote');
                    requestQuote(opp, quotedDate, quoteId, innerDone);
                });
            },

            //Step 3: Support Quote Completed state
            function(cb) {
                if (salesStage != 'Quote Completed') return cb();
                var innerDone = _.after(opps.length, cb);

                _.each(opps, function(opp){
                    h.log('debug', '[' + oppId + '] completing quote');
                    completeQuote(opp, quotedDate, quoteId, innerDone);
                });
            },

            //Step 4: Support Quote Delivered state
            function(cb) {
                if (salesStage != 'Quote Delivered') return cb();
                var innerDone = _.after(opps.length, cb);

                _.each(opps, function(opp){
                    completeQuote(opp, quotedDate, quoteId, function(err, quote) {
                        if (err) return innerDone(err);
                        h.log('debug', '[' + oppId + '] delivering quote');
                        deliverQuote(opp, quote, quotedDate, innerDone);
                    });
                });
            },

            //Step 4: Support Customer Commitment or PO Received without required info state
            function(cb) {
                if (salesStage == 'Customer Commitment' || (salesStage == 'PO Received' && (!poDate || !poNumber))) {
                    var innerDone = _.after(opps.length, cb);

                    _.each(opps, function(opp){
                        completeQuote(opp, quotedDate, quoteId, function(err, quote) {
                            if (err) return innerDone(err);
                            deliverQuote(opp, quote, quotedDate, function(err, quote) {
                                if (err) return innerDone(err);

                                h.log('debug', '[' + oppId + '] commiting quote');
                                commitQuote(opp, quote, quotedDate, forecastedCloseDate, innerDone);
                            });
                        });
                    });
                } else {
                    return cb();
                }
            },

            //Step 5: Support PO Received state
            function(cb) {
                if (salesStage == 'PO Received' && poDate && poNumber) {
                    var innerDone = _.after(opps.length, cb);

                    _.each(opps, function(opp) {
                        h.log('debug', '[' + oppId + '] poReceived opp');
                        poReceived(opp, quotedDate, quoteId, poAmount, poDate, poNumber, resultReason, innerDone);
                    });
                } else {
                    return cb();
                }
            },

            //Step 6: Support Closed Sale state
            function(cb) {
                if (salesStage == 'Closed Sale' && poDate && poNumber && soNumber && soDate) {
                    var innerDone = _.after(opps.length, cb);

                    _.each(opps, function(opp) {
                        h.log('debug', '[' + oppId + '] closedSale opp');
                        if (opp.flows.salesStages.state.name == 'poReceived')
                            completeDistiBooking(opp, null, resolutionDate, poAmount, poDate, poNumber, soDate, soNumber, innerDone);
                        else
                            resolveAsWin(opp, quoteId, resolutionDate, poAmount, poDate, poNumber, resultReason, soDate, soNumber, innerDone);
                    });
                } else {
                    return cb();
                }
            },

            //Step 7: Support House Account and No Service state
            function(cb) {
                if (salesStage != 'House Account' && salesStage != 'No Service') return cb();
                var innerDone = _.after(opps.length, cb);

                _.each(opps, function(opp) {
                    h.log('debug', '[' + oppId + '] house account or no service opp');
                    resolveAsLoss(opp, resolutionDate, resultReason, innerDone);
                });
            },

        ], done);

    });

}

h.log('', 'Processing ' + input.file);
init(function(err) {
    csvHelper.readAsObj(input.file, function (data) {
        data = _.rest(data, input.skip);
        var rowNum = input.skip;

        async.eachLimit(data, input.limit,
            function(csv, cb) {
                doTheWork(csv, ++rowNum, cb);
            },
            function (err) {
                h.log('', 'DONE ' + err);
            }
        );
    });
});

