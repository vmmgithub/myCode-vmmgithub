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
    .alias('o', 'offerSearchBy').describe('o', 'Search by attribute on offers [_id, displayName]').default('o', '_id')
    .alias('r', 'revert').describe('r', 'revert to sales stage as quoteDelivered, in order to split').default('r', false)
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    offerCollection = restApi.getCollection(restApi, "app.offers"),
    csvHelper = new csvHelperInstance();

var findOpportunity = function (value, callback) {
    h.findRecords(oppCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var findOffers = function (opportunity, callback) {
    tenantApi.execute('app.opportunities', opportunity._id, 'findOffers', {filter: {}, configParams: {findActionParams: {criteria: 'initial'}}, params: {limit: 10000, columns: ['_id']}}, function(err, res) {
        if (err || res.success != true || !res || !res.data) 
            return callback("on findOffers " + JSON.stringify(err));
        callback(null, res.data['app.offer'] || []);
    });
};

var revertStage = function (opportunity, callback) {
    tenantApi.execute('app.opportunities', opportunity._id, 'resetToQuoteDelivered', {}, function(err, res) {
        if (err || res.success != true || !res) return callback("on revertStage " + JSON.stringify(err));
        callback(null, res);
    });
};

var moveOffers = function (opportunity, offerIds, callback) {
    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        }, 
        selections: []
    };

    _.each(offerIds, function(offerId) {
        payload.selections.push({_id: offerId});
    });

    tenantApi.execute('app.opportunities', opportunity._id, 'getMoveOffersInput', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['app.move.offers.input'] || !res.data['app.move.offers.input'][0]) 
            return callback("on move input " + JSON.stringify(err));

        tenantApi.execute('app.opportunities', opportunity._id, 'moveOffers', res.data['app.move.offers.input'][0], function(err, res) {
            if (err || res.success == false) 
                return callback(" setting loss input " + JSON.stringify(err || res));

            return callback(null, res.data && res.data['app.opportunity'] && res.data['app.opportunity'][1]);
        });
    });
};

var prepMove = function(opportunity, offerIds, ocb) {
    findOffers(opportunity, function(err, offers) {
        if (err) return ocb(err);

        var matchedOfferIds = [];
        _.each(offers, function(offer) {
            if(!_.isEmpty(_.intersection(offerIds, [h.getObjectValueFromPath(offer, input.offerSearchBy)])))
                matchedOfferIds.push(offer._id);
        });

        if (_.isEmpty(matchedOfferIds)) {
            h.log('warn', 'No offers to move ' + opportunity._id);
            return ocb(null, opportunity);
        }

        moveOffers(opportunity, matchedOfferIds, function(err, newOpp) {
            if (!err) h.log('info', "Splitting opportunity '" + opportunity._id + "' with " + (newOpp && (newOpp._id + ' ' + newOpp.displayName)));
            return ocb(err, newOpp);
        });
    });    
}

var processRecord = function (oppName, offerIds, callback) {
    var done = function(err, newOpp) {
        if (err) {
            h.log('error', "Splitting opportunity '" + oppName + "': " + JSON.stringify(err));
            h.print('FAIL|', [oppName, _.reduce(offerIds, function(str, c) { if(str == '') return c; else return str + ',' + c}, '')]);
        }
        callback();
    };

    findOpportunity(oppName, function(err, opps) {
        if (err) return done(err);

        async.eachLimit(opps, 1, function(opportunity, ocb) {

            var stage = h.getFlowState(opportunity, 'salesStages');
            if (stage == 'poReceived' || stage == 'closedSale') {
                if (!input.revert) return ocb('Opportunity in wrong sales stage ' + stage);

                revertStage(opportunity, function(err, res) {
                    prepMove(opportunity, offerIds, ocb);
                });
            } else {
                prepMove(opportunity, offerIds, ocb);                
            }

        },
        done);
    });
};

h.log('info', 'Processing ' + input.file);
h.print('FAIL|', ['Opportunity', 'OfferIds']); // for auto re-processing

csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();
            var oppName = csvRecord["Opportunity"];
            var offerIds = csvRecord["OfferIds"];

            if (oppName && offerIds && offerIds.split(',').length > 0) {
                offerIds = offerIds.split(',');
                processRecord(oppName, offerIds, callback);
            } else {
                h.log('warn', 'Skipping ' + oppName + ' and ' + offerIds);
                callback();
            }
        },
        function (err) {
            h.log('info', "DONE " + err);
        });
});
