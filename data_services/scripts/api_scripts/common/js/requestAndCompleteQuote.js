#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
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
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalId]').default('b', '_id')
    .alias('d', 'directory').describe('d', 'directory with all the quote attachments').default('d', '/Users/nbose/Documents/temp/saas_mb_sample')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    csvHelper = new csvHelperInstance();

var findSource = function (customerNumber, sellingPeriod, businessLine, callback) {
    var filter = {};
    filter['extensions.tenant.customerNumber.value'] = {"$regex": "^" + customerNumber};;
    filter['extensions.master.targetPeriod.value.name'] = sellingPeriod.toLowerCase();
    filter['extensions.master.businessLine.value.name'] = businessLine.toLowerCase();

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

var processRecord = function (customerNumber, sellingPeriod, businessLine, margin, filePath, callback) {
    var done = function(err) {
        if (err) 
            h.log('error', "Quoting opportunity '" + customerNumber + "': " + JSON.stringify(err));
        else 
            h.log('info', "Quoting opportunity '" + customerNumber + "'");

        return callback();
    };

    findSource(customerNumber, sellingPeriod, businessLine, function(err, res) {
        if (err) return done(err);

        if (res && res[0] && res[0].flows.salesStages.state.name == 'quoteCompleted') {
            h.log('warn', 'Skipping opportunity ' + customerNumber + ' because it is not in the notContacted state');
            return done();
        }

        if (!filePath || !fs.existsSync(filePath)) {
            h.log('warn', 'Skipping opportunity ' + customerNumber + ' because the file does not exist');
            return done();
        }

        uploadDocument(tenantApi, filePath, function(err, uploadedDocument) {
            if (err) return done(err);

            async.eachLimit(res, 1, function(opportunity, ocb) {

                getQuoteInput(opportunity, function(err, input) {
                    if (err) return done(err);

                    requestQuote(opportunity, input, function(err, quote) {
                        if (err) return done(err);
                    
                        getCreateQuoteInput(quote, function(err, quoteInput) {
                            if (err) return done(err);

                            createQuote(quote, quoteInput, margin, uploadedDocument, ocb);
                        });
                    });
                });
            },
            done);
        });

    });
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();

        var customerNumber = csvRecord["Customer Number"];
        var sellingPeriod = csvRecord["Selling Period"];
        var businessLine = csvRecord["Business Line"];
        var margin = Number(csvRecord["Margin"]);
//        var filePath = csvRecord["Path"] || input.directory + '/' + customerNumber + '-' + businessLine + '-' + sellingPeriod + '.xlsx';
        var filePath = input.directory + '/' + csvRecord["Path"] ;

        if (customerNumber) {
            processRecord(customerNumber, sellingPeriod, businessLine, margin, filePath, callback);
        } else {
            h.log('warn', 'Skipping ' + customerNumber + ' because of missing data');
            callback();
        }
    },
    function (err) {
        h.log('info', "DONE " + err);
    });
});

