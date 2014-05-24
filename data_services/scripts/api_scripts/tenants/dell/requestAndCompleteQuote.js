#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('../../common/js/helper');
var log = console.log;
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility requests and completes a quote in Renew, using a CSV input.\
        \n\nThe input file should contain five columns: Opportunity, Margin and Path where: \
        \n "Opportunity" has the name or search criteria of opportunity that needs to be quoted\
        \n "FilePath" has the name of path to the file that needs to attached with the quote\
        \n "Margin" has the name of opportunity that needs to be quoted\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port')
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password')
    .alias('f', 'file').describe('f', 'File to process')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', 'displayName')
    .alias('d', 'directory').describe('d', 'directory with all the quote attachments').default('d', '/Users/nbose/Documents/temp/saas_mb_sample')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    csvHelper = new csvHelperInstance();

var findSource = function (value, callback) {
    var filter = {};
    filter[input.searchBy] = value;
    filter['flows.salesStages.state.name'] = "notCompleted";

    h.findRecords(oppCollection, {
        multiple: input.multiple,
        filter: input.filter,
    }, callback);
};

var getQuoteInput = function (opportunity, callback) {
    var payload = {
        detail: {
            _id: opportunity._id,
            displayName: opportunity.displayName,
            type: opportunity.type
        },
        selections: []
    };

    tenantApi.execute('app.opportunities', opportunity._id, 'getQuoteInput', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['app.quote.input'] || !res.data['app.quote.input'][0]) 
            return callback("on getQuoteInput " + JSON.stringify(err || res));

        return callback(null, res.data['app.quote.input'][0]);
    });
};

var requestQuote = function (opportunity, quoteInput, callback) {
    var payload = quoteInput;
    quoteInput.resultReason = {name: 'initialQuote'}; //Hard coded reason

    tenantApi.execute('app.opportunities', opportunity._id, 'requestQuote', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['app.quote'] || !res.data['app.quote'][0]) 
            return callback("on requestQuote " + JSON.stringify(err || res));

        return callback(null, res.data['app.quote'][0]);
    });
};

var getCreateQuoteInput = function (quote, callback) {
    var payload = {
        detail: {
            _id: quote._id,
            displayName: quote.displayName,
            type: quote.type
        },
        selections: []
    };

    tenantApi.execute('app.quotes', quote._id, 'getCreateQuoteInput', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['app.create.quote.input'] || !res.data['app.create.quote.input'][0]) 
            return callback("on getCreateQuoteInput " + JSON.stringify(err || res));

        return callback(null, res.data['app.create.quote.input'][0]);
    });
};

var createQuote = function (quote, quoteInput, margin, uploadedDocument, callback) {
    quoteInput.margin = margin;
    if (uploadedDocument) 
        quoteInput.documents = [{
            documentName: decodeURIComponent(uploadedDocument.name),
            link: uploadedDocument.url,
            delete_link: uploadedDocument.delete_url,
            date: uploadedDocument.date,
            type: 'core.related.document',
        }];

    tenantApi.execute('app.quotes', quote._id, 'createQuote', quoteInput, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['app.quote'] || !res.data['app.quote'][0]) 
            return callback("on createQuote " + JSON.stringify(err || res));

        return callback(null, res.data['app.quote'][0]);
    });
};

var processRecord = function (value, filePath, callback) {
    var done = function(err) {
        if (err) 
            log('error', "Quoting opportunity '" + customerNumber + "': " + JSON.stringify(err));
        else 
            log('info', "Quoting opportunity '" + customerNumber + "'");

        return callback();
    };

    log('debug', "Step 0 done for " + customerNumber);
    findSource(value, function(err, res) {
        if (err) return done(err);

        if (res && res[0] && res[0].flows.salesStages.state.name == 'quoteCompleted') {
            log ('info', 'Skipping opportunity ' + customerNumber + ' because it is not in the notContacted state');
            return done();
        }

        if (!filePath || !fs.existsSync(filePath)) {
            log ('info', 'Skipping opportunity ' + customerNumber + ' because the file does not exist');
            return done();
        }

        log('debug', "Step 1 done for " + customerNumber);
        uploadDocument(tenantApi, filePath, function(err, uploadedDocument) {
            if (err) return done(err);

            log('debug', "Step 2 done for " + customerNumber);
            async.eachLimit(res, 1, function(opportunity, ocb) {

                log('debug', "Step 3 done for " + customerNumber);
                getQuoteInput(opportunity, function(err, input) {
                    if (err) return done(err);

                    log('debug', "Step 4 done for " + customerNumber);
                    requestQuote(opportunity, input, function(err, quote) {
                        if (err) return done(err);
                    
                        log('debug', "Step 5 done for " + customerNumber);
                        getCreateQuoteInput(quote, function(err, quoteInput) {
                            if (err) return done(err);

                            log('debug', "Step 6 done for " + customerNumber);
                            createQuote(quote, quoteInput, margin, uploadedDocument, ocb);
                        });
                    });
                });
            },
            done);
        });

    });
};

log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();

        var oppName = csvRecord["Source"];
        var filePath = input.directory + '/' + csvRecord["Path"] ;

        if (value) {
            processRecord(value, filePath, callback);
        } else {
            log('Skipping ' + value + ' because of missing data');
            callback();
        }
    },
    function (err) {
        log('info', "DONE " + err);
    });
});

