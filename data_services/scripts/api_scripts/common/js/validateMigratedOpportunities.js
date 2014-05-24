#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require('../../lib/helpers/CsvHelper');

var input = require('optimist')
    .usage('\nREADME: This is a utility used for scrubbing opportunities in Renew, based on Atlas values.\
        \n\nThe input file should contain five columns: ...... where: \
        \n Opportunity Id has the externalId, of opportunity that needs to be updated\
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
    contactsCollection = h.getCollection(restApi, 'core.contacts'),
    csvHelper = new csvHelperInstance();

var oppId;

var findOpportunity = function (value, callback) {
    h.findRecords(oppCollection, {
        multiple: input.multiple,
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var findSalesRep = function (xid, callback) {
    if (!xid) return callback();
    var filter = {};
    filter['externalIds.id'] = {'$regex': '^' + xid};

    h.findCachedRecords(contactsCollection, {
        filter: filter,
    }, function(err, records) {
        	return callback(null, h.getTargetPointer(records ));        
    });
};

var findOffers = function (opportunity, callback) {
    var payload = {
        filter: {},
        params: {
            columns: ["_id", "displayName"]
        }
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'findOffers', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data)
            return callback("on offer " + JSON.stringify(err));

        return callback(null, res.data['app.offer'] || res.data['unknown model']);
    });
};

var validateRepAndOffers = function(opportunity, offerCount, salesRep,  callback) {
    findOffers(opportunity, function(err, offers) {
        if (err) {
            h.log('warn', JSON.stringify(err));
            return callback(false);
        }

        var match = true;
        if (offers.length != offerCount) {
            match = false;
            h.print('warn', [oppId, 'ASSETCOUNT', offers.length, offerCount]);
        }

        if (!salesRep) return callback(match);
        var existingRep = h.getRel(opportunity, 'salesRep');
      

        findSalesRep(salesRep,  function(err, rep) {
        rep = rep && rep[0];
            if (!rep) {
               match = false;
               h.print('error', [oppId, 'SALESREP', existingRep.key, "NoSalesRepFound=>"+salesRep]);
            }
            else
            if (rep || existingRep.key != rep.key) {
                match = false;
                h.print('warn', [oppId, 'SALESREP', existingRep.key, rep]);
            }

            callback(match);
        });
    });
};

var doTheWork = function (csvRecord, rowNum, callback) {
    if (!csvRecord) return callback();

    oppId = csvRecord['opportunityid'];
    var salesStage = csvRecord['ssisalesstage'];
    var commitLevel = csvRecord['commitlevel'];
    var resolutionDate = h.getCSVDate(csvRecord, 'resolutiondate');
    var contactedDate = h.getCSVDate(csvRecord, 'firstcontactdate');
    var quotedDate = h.getCSVDate(csvRecord, 'firstquotedate');
    var poDate = h.getCSVDate(csvRecord, 'ssibookingdate');
    var soDate = h.getCSVDate(csvRecord, 'clientbookingdate');
    var poNumber = csvRecord['newponumber'];
    var poAmount = csvRecord['localtransactionamount'];
    var soNumber = csvRecord['newsonumber'];
    var resultReason = csvRecord['ssiresultreason'];
    var amount = csvRecord['localtransactionamount'];
    var salesRep = csvRecord['SalesRep'];
    var offerCount = csvRecord['AssetCount'];

    if (resultReason == '<none>') resultReason = null;
    if (salesStage == 'Quote Request') salesStage = 'Quote Requested';
    if (contactedDate && resolutionDate && contactedDate.isAfter(resolutionDate)) contactedDate = resolutionDate;
    if (quotedDate && resolutionDate && quotedDate.isAfter(resolutionDate)) quotedDate = resolutionDate;

    if (!oppId) {
        h.log('info', 'Skipping ' + csvRecord.toString() + ' because of missing data');
        return callback();
    }

    findOpportunity(oppId, function(err, res) {
        if (err || !res || res.length == 0) {
            h.print('error', [oppId, 'ORPHAN', JSON.stringify(err)]);
            return callback();
        }

        var opp = _.find(res, function(o) { return !o.isSubordinate; }),
            oppName = opp && opp.displayName,
            match = true;

        if (!opp) {
            match = false;
            h.print('error', [oppId, 'ORPHAN', ]);
            return callback();
        }

        if (opp.flows.salesStages.state.displayName != salesStage) {
            match = false;
            h.print('warn', [oppId, 'STAGE', salesStage, opp.flows.salesStages.state.displayName]);
        } 

        if (!_.isEmpty(resolutionDate) && resolutionDate.toISOString() != 'Invalid date' && h.compareWithDayOffset(resolutionDate, opp.resolutionDate)) {
            match = false;
            h.print('warn', [oppId, 'RESDT', resolutionDate.toISOString(), opp.resolutionDate]);
        } 

        if (salesStage != 'House Account' && salesStage != 'No Service' && h.compareWithAmountOffset(amount, opp.amount.amount)) {
            match = false;
            h.print('warn', [oppId, 'AMOUNT', h.toFixed(amount), h.toFixed(opp.amount.amount)]);
        } 

        validateRepAndOffers(opp, offerCount, salesRep, function(m) {
            match = m && match;

            if (match) {
                h.print('info', [rowNum, oppId, oppName, salesStage]);
            } else {
                h.print('error', [rowNum, oppId, oppName, salesStage]);
                h.print('debug', _.values(csvRecord));
            }

            return callback();
        });

    });

}

h.log('', 'Processing ' + input.file);
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
