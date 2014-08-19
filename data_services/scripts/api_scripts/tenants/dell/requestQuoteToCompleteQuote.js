#!/usr/bin/env node

var fs = require("fs");
var _ = require("underscore");
var async = require("async");
var h = require('../../common/js/helper');
var inflection = require('inflection');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");
var RestApiInterface = require('../../lib/helpers/RestApi');

var log = function(mode, p) {
    if (mode == 'debug') return;

    var pre = '[' + (new Date()).toISOString() + '] [' + mode + '] ';
    console.log(pre + p);
}

var input = require('optimist')
    .usage('\nREADME: This is a utility completes quote in Renew, using a CSV input.\
        \nIt requires opportunity in <quoteRequested> stage and quote status should be <requested>. \
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
    .alias('m', 'multiple').describe('m', 'Flag to indicate if updating all matching records or just the first').boolean('m').default('m', false)
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('d', 'directory').describe('d', 'directory with all the quote attachments').default('d', '/Users/nbose/Documents/temp/saas_mb_sample')
    .demand(['h', 't', 'f'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    oppCollection = h.getCollection(restApi, "app.opportunities"),
    quotesCollection = h.getCollection(restApi, "app.quotes"),
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

            var res;
            try {
                res = JSON.parse(body);
                callback(null, res[0]);
            } catch (err) {
                return callback(err);
            }
        });
    } catch (e) {
        return callback(e, null);
    }
};

var getCreateQuoteInput = function (quote, opportunity, callback) {
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
            return callback("on getCreateQuoteInput : Error = "+ err +" | " + JSON.stringify(err || res));

        return callback(null, res.data['app.create.quote.input'][0]);
    });
};

var addIsPreQuote = function(quote, callback) {
    if (!quote) callback(null, quote);

    if (!quote.extensions) quote.extensions = {};
    if (!quote.extensions.tenant) quote.extensions.tenant = {};
    if (!quote.extensions.tenant.isPrequote) quote.extensions.tenant.isPrequote = {};
    quote.extensions.tenant.isPrequote.value = true;

    quotesCollection.update(quote, callback);
};

var createQuote = function (quote, opportunity, quoteInput, margin, uploadedDocument, callback) {

    quoteInput.margin = margin;
    quoteInput.isPrequote = false;;
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
            return callback("on createQuote :  " + JSON.stringify(err || res));

        return callback(null, res.data['app.quote'][0]);
    });
};


// Select the quote if exists in the order of Primary, latest, quote  and base
var scanQuotes = function (opp, callback) {


      var quoteIds = (!_.isEmpty(h.getRelKeys(opp, 'primaryQuote')) ? h.getRelKeys(opp, 'primaryQuote') :
                    (!_.isEmpty(h.getRelKeys(opp, 'latestQuote')) ? h.getRelKeys(opp, 'latestQuote')   :
                    (!_.isEmpty(h.getRelKeys(opp, 'quote')) ?  h.getRelKeys(opp, 'quote') :
                    h.getRelKeys(opp, 'baseQuote') ) ) );

    if (!quoteIds) { return callback; }
   
    var filter = {};
    filter['_id'] = quoteIds;
    quotesCollection.find(filter , {}, function (err, records) {
        return callback(err, records[0] );
    });

};

var processRecord = function (oppId, margin, filePath, callback) {
    var done = function(err) {
        if (err) 
            log('error', "Quoting opportunity '" +  oppId + '::' + margin + '::' + filePath +  "' :: JSON: " + JSON.stringify(err));
        else 
            log('info', "Quoting opportunity '" +  oppId + '::' + margin + '::' + filePath +  "'");

        return callback();
    };

    log('debug', "Step 0 done for " + oppId);
    findSource(oppId,  function(err, res) {
        if (err) return done(err);
        
        if (res && res[0] && res[0].flows.salesStages.state.name != 'quoteRequested')  {
            log ('info', 'Skipping opportunity ' + oppId + '::' + margin + '::' + filePath + ' ::  because it is not in the quoteRequest state');
            return done();
        }


        if (!filePath || !fs.existsSync(filePath)) {
            log ('info', 'Skipping opportunity ' + oppId + '::' + margin + '::' + filePath + ' :: because the file does not exist');
             return done();
         }


        log('debug', "Step 1 done for " + oppId);
        uploadDocument(filePath, function(err, uploadedDocument) {
            if (err) return done(err);

            log('debug', "Step 2 done for " + oppId);
            async.eachLimit(res, 1, function(opportunity, ocb) {

                    log('debug', "Step 4 done for " + oppId);
                    scanQuotes(opportunity, function(err, quote) {
                        if (err) return done(err);
                    
                        log('debug', "Step 5 done for " + oppId);
                        getCreateQuoteInput(quote, opportunity, function(err, quoteInput) {
                            if (err) return done(err);

                            log('debug', "Step 6 done for " + oppId);
                            createQuote(quote, opportunity,quoteInput,  margin, uploadedDocument, function(err, quote){
                                if (err) return done(err);
                                addIsPreQuote(quote, ocb);
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

        var oppId = csvRecord["Opportunity Id"];
        var margin = Number(csvRecord["Margin"]);
        var filePath = input.directory + '/' + csvRecord["Path"] ;

        var filePath = input.directory + '/' + csvRecord["Path"] ;
//        console.log("FILE ===>", filePath);

        if (oppId) {
            processRecord(oppId, margin, filePath, callback);
        } else {
            log('Skipping ' + oppId + ' because of missing data');
            callback();
        }
    },
    function (err) {
        log('info', "DONE " + err);
    });
});

