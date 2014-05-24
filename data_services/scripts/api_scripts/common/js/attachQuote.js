#!/usr/bin/env node

var fs = require("fs");
var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility requests and completes a quote in Renew, using a CSV input.\
        \n\nThe input file should contain five columns: Name, Amount, Currency, Margin and Path where: \
        \n "QuoteId" has the name or search criteria of quote to which the document will be attached \
        \n "QuotePath" has the path to the file that needs to attached with the quote\
        \n "ResellerQuotePath" has the path to the reseller file that needs to attached with the quote\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port')
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'file').describe('f', 'File to process')
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('b', 'searchBy').describe('b', 'Search by attribute [_id, displayName, externalIds.id]').default('b', '_id')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    quoteCollection = h.getCollection(restApi, "app.quotes"),
    csvHelper = new csvHelperInstance();

var findQuote = function (value, callback) {
    h.findRecords(quoteCollection, {
        searchBy: input.searchBy,
        value: value,
    }, callback);
};

var attachQuoteFile = function (quote, quoteDocument, resellerDocument, callback) {

    if (quoteDocument) {
        if (!quote.attachedQuotes) quote.attachedQuotes = [];
        quote.attachedQuotes.push({
            documentName: decodeURIComponent(quoteDocument.name),
            link: quoteDocument.url,
            delete_link: quoteDocument.delete_url,
            date: quoteDocument.date,
            type: 'core.related.document',
        });
    }

    if (resellerDocument) {
        if (!quote.resellerDocuments) quote.resellerDocuments = [];
        quote.resellerQuotes.push({
            documentName: decodeURIComponent(resellerDocument.name),
            link: resellerDocument.url,
            delete_link: resellerDocument.delete_url,
            date: resellerDocument.date,
            type: 'core.related.document',            
        });
    }

    quoteCollection.update(quote, callback);
};

var processRecord = function (quoteId, filePath, resellerFilePath, callback) {
    var done = function(err) {
        if (err) 
            h.log('error', "Attaching quote file '" + quoteId + "': " + JSON.stringify(err));
        else 
            h.log('info', "Attaching quote '" + quoteId + "'");

        return callback();
    };

    findQuote(quoteId, function(err, res) {
        if (err) return done(err);

        h.uploadDocument(tenantApi, filePath, function(err, quoteDocument) {
            if (err) return done(err);

            h.uploadDocument(tenantApi, resellerFilePath, function(err, resellerDocument) {
                if (err) return done(err);

                async.eachLimit(res, 1, function(quote, ocb) {
                    attachQuoteFile(quote, quoteDocument, resellerDocument, ocb);
                },
                done);
            });
        });
    });
};

h.log('info', 'Processing ' + input.file);
csvHelper.readAsObj(input.file, function (data) {
    async.eachLimit(data, input.limit, function (csvRecord, callback) {
        if (!data) return callback();

        var quoteId = csvRecord["QuoteId"];
        var filePath = csvRecord["QuotePath"];
        var resellerFilePath = csvRecord["ResellerQuotePath"];

        if (quoteId && (filePath || resellerFilePath)) {
            processRecord(quoteId, filePath, resellerFilePath, callback);
        } else {
            h.log('warn', 'Skipping ' + quoteId + ' because of missing data');
            callback();
        }
    },
    function (err) {
        h.log('info', 'DONE ' + err);
    });
});
