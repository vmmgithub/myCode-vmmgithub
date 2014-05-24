#!/usr/bin/env node

var fs = require("fs");
var _ = require("underscore");
var async = require("async");
var h = require('../../common/js/helper');
var inflection = require('inflection');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");
var RestApiInterface = require('../../lib/helpers/RestApi');
var deliverFlag;

var input = require('optimist')
    .usage('\nREADME: This is a utility requests and completes a quote in Renew, using a CSV input.\
        \n\nThe input file should contain three columns: Opportunity Id, Margin and Path where: \
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
    .alias('v', 'deliver').describe('v', 'Quote Delivered if set to true').boolean('v').default('v', false)
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('d', 'directory').describe('d', 'directory with all the quote attachments').default('d', '/Users/nbose/Documents/temp/saas_mb_sample')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    csvHelper = new csvHelperInstance();

var findSource = function (oppId, callback) {
    var filter = {};

/*  Following filter conditions are not required 
    filter['extensions.tenant.customerNumber.value'] = {"$regex": "^" + customerNumber};;
    filter['extensions.master.targetPeriod.value.name'] = sellingPeriod.toLowerCase();
    filter['extensions.master.businessLine.value.name'] = businessLine.toLowerCase();
*/
    filter['_id'] = oppId;
    oppCollection.find(filter, {}, function(err, records) {
        if (err || !records || records.length == 0) {
            return callback(err || 'No opp records found');            
        }

        if (records.length > 1) {
            if (input.multiple) return callback(null, records);
            else return callback("Found multiple records " + records.length);
        }
            
        return callback(null, [records[0]]);
    });
};

var uploadDocument = function (filePath, callback) {
    try {
        var readStream = fs.createReadStream(filePath);
        readStream.on("error", function (err) {
            return callback(err, null);
        });

        tenantApi.attachment(readStream, function (err, resp, body) {
            if (err) return callback(err, null);

            var res = JSON.parse(body);
            callback(null, res[0]);            
        });
    } catch (e) {
        return callback(e, null);
    }
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
        if (err || !res || !res.success || !res.data || !res.data['app.quote.input'] || !res.data['app.quote.input'][0]) 
            return callback("on getQuoteInput " + JSON.stringify(err || res));

        return callback(null, res.data['app.quote.input'][0]);
    });
};

var requestQuote = function (opportunity, quoteInput, callback) {
    var payload = quoteInput;
    quoteInput.resultReason = {name: 'initialQuote'}; //Hard coded reason

    //Suppress emails to reps
    if (!quoteInput.extensions) quoteInput.extensions = {};
    if (!quoteInput.extensions.tenant) quoteInput.extensions.tenant = {};
    if (!quoteInput.extensions.tenant.isPrequote) quoteInput.extensions.tenant.isPrequote = {};
    quoteInput.extensions.tenant.isPrequote.value = true;

    tenantApi.execute('app.opportunities', opportunity._id, 'synchCompleteQuote', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.quote'] || !res.data['app.quote'][0]) 
            return callback("on requestQuote " + JSON.stringify(err || res));

        return callback(null, res.data['app.quote'][0]);
    });
};

var getCreateQuoteInput = function (opportunity, quote, callback) {
    var payload = {
        detail: {
            _id: quote._id,
            displayName: quote.displayName,
            type: quote.type
        },
        selections: []
    };

    tenantApi.execute('app.quotes', quote._id, 'getCreateQuoteInput', payload, function(err, res) {
        if (err || !res || !res.success || !res.data || !res.data['app.create.quote.input'] || !res.data['app.create.quote.input'][0]) 
            return callback("on getCreateQuoteInput " + JSON.stringify(err || res));

        return callback(null, res.data['app.create.quote.input'][0]);
    });
};

var createQuote = function (opportunity, quote, quoteInput, margin, uploadedDocument, callback) {

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
        if (err || !res || !res.success || !res || !res.data || !res.data['app.quote'] || !res.data['app.quote'][0]) 
            return callback("on createQuote " + JSON.stringify(err || res));

        return callback(null, res.data['app.quote'][0]);
    });
};

var deliverQuote = function (opportunity, quote, callback) {
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
        input.deliveryDate = moment();

        tenantApi.execute('app.quotes', quote._id, 'sendQuote', input, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['app.quote'] || !res.data['app.quote'][0])
                return callback("on sendQuote " + JSON.stringify(err || res));

            callback(err, res.data['app.quote'][0]);
        });
    });
};

var processRecord = function (oppId, margin, filePath, callback) {
    var done = function(err) {
        if (err) 
            h.log('error', "Quoting opportunity '" +  oppId + '::' + margin + '::' + filePath +  "' :: JSON: " + JSON.stringify(err));
        else 
            h.log('info', "Quoting opportunity '" +  oppId + '::' + margin + '::' + filePath +  "'");

        return callback();
    };

    findSource(oppId,  function(err, res) {
        if (err) return done(err);

        if ((res && res[0] && res[0].flows.salesStages.state.name == 'quoteCompleted') ||  (res && res[0] && res[0].flows.salesStages.state.name == 'quoteRequested')) {
            h.log('info', 'Skipping opportunity ' + oppId + '::' + margin + '::' + filePath + ' ::  because it is not in the notContacted state');
            return done();
        }

        if (!filePath || !fs.existsSync(filePath)) {
            h.log('info', 'Skipping opportunity ' + oppId + '::' + margin + '::' + filePath + ' :: because the file does not exist');
             return done();
         }

        uploadDocument(filePath, function(err, uploadedDocument) {
            if (err) return done(err);

            async.eachLimit(res, 1, function(opportunity, ocb) {
                getQuoteInput(opportunity, function(err, input) {
                    if (err) return done(err);

                    requestQuote(opportunity, input, function(err, quote) {
                        if (err) return done(err);
                    
                        getCreateQuoteInput(opportunity, quote, function(err, quoteInput) {
                            if (err) return done(err);

                            createQuote(opportunity, quote, quoteInput, margin, uploadedDocument, function(err, quote) {
                                if (err) return done(err);
                                if (deliverFlag)
                            		deliverQuote(opportunity, quote, ocb);
                                else 
                                    ocb();
                            });
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

        var oppId = csvRecord["Opportunity Id"];
        var margin = Number(csvRecord["Margin"]);
        var filePath = input.directory + '/' + csvRecord["Path"] ;

        var filePath = input.directory + '/' + csvRecord["Path"] ;
        deliverFlag = input.deliver;
        if (oppId) {
            processRecord(oppId, margin, filePath,  callback);
        } else {
            h.log('warn', 'Skipping ' + oppId + ' because of missing data');
            callback();
        }
    },
    function (err) {
        h.log('info', "DONE " + err);
    });
});

