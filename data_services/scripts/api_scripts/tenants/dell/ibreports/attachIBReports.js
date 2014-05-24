#!/usr/bin/env node

var _ = require("underscore");
var fs = require("fs");
var async = require("async");
var commander = require("commander");
var d = require('./date.js');

var RestApiInterface = require('./../lib/helpers/RestApi');

var log = function(s) {
    var d = new Date();
    console.log('[' + d.format("isoDateTime") + '] ' + s);
}

commander.version("1.0")
    .option("-h, --host [s]", "Scpecify Host")
    .option("-p, --port [s]", "Scpecify Port")
    .option("-i, --start <n>", "Starting File ", parseInt)
    .option("-u, --user [s]", "Scpecify User")
    .option("-up, --user-password [s]", "Scpecify User Password")
    .option("-d, --directory [s]", "Scpecify Directory")
    .option("-t, --tenant [s]", "Scpecify Tenant");

commander
    .command("checkDocumentContacts")
    .description("execute checkDocumentContacts")
    .action(function (cmd) {
        Scrub(true).checkDocumentContacts();
    });

commander
    .command("attachDocuments")
    .description("execute mapDocuments")
    .action(function (cmd) {
        Scrub(true).attachDocuments();
    });

commander.parse(process.argv);

function Scrub(dryRun) {
    var restApi = new RestApiInterface(commander.host, commander.port, commander.user, commander.userPassword);
    var tenant = restApi.setTenant(commander.tenant);
    var startNumber = commander.start;
    var contactCollection = restApi.getCollection("core.contacts", "core.contact");

    var DOCUMENT_EXTENSION_NAME = "IBReport";
    var DOCUMENTS_DIR = commander.directory.split(",");
    log("Processing directory " + DOCUMENTS_DIR + " with starting file " + startNumber);

    var readDocumentDir = function (next, end) {
        async.forEachSeries(DOCUMENTS_DIR, function (dir, nextDir) {
            fs.readdir(dir, function (err, files) {
                next(err, files, dir, nextDir);
            });
        }, end);
    }

    var getCustomerNumber = function (file) {
        var pattern = /_([a-z0-9A-Z]+)_[\W\w]+/;
        return file.split('_')[1];
    }

    var findContacts = function (customerNumber, callback) {
        var start = 0;
        var limit = 5;
        var contacts = [];
        var findPart = function () {
            var filter = {
                "type": "core.contact/organization",
                "extensions.tenant.customerNumber.value": customerNumber
            };

            contactCollection.find(filter, {
                start: start,
                limit: limit
            }, function (err, records) {
                if (err) {
                    callback(err);
                } else {
                    if (records && records.length) {
                        contacts = contacts.concat(records);
                        if (records.length != limit) {
                            callback(null, contacts);
                        } else {
                            start += limit;
                            findPart();
                        }
                    } else {
                        callback(null, contacts);
                    }
                }
            });
        }
        findPart();
    }

    var uploadDocument = function (filePath, callback) {
        var readStream = fs.createReadStream(filePath);
        readStream.on("error", function (err) {
            callback(err, null);
        });
        try {
            tenant.attachment(readStream, function (error, response, body) {
                if (error) {
                    callback(error, null);
                } else {
                    try {
                        var res = JSON.parse(body);
                        var file = res[0];
                        callback(null, file);
                    } catch (e) {
                        log(e);
                        callback(e, null);
                    }
                }
            });
        } catch (e) {
            callback(e, null);
        }
    }

    var updateContact = function (contact, uploadedDocument, callback) {
        if (!contact.extensions) {
            contact.extensions = {}
        }
        if (!contact.extensions.tenant) {
            contact.extensions.tenant = {};
        }
        if (!contact.extensions.tenant[DOCUMENT_EXTENSION_NAME]) {
            contact.extensions.tenant[DOCUMENT_EXTENSION_NAME] = {
                type: "core.related.document"
            }
        }

        contact.extensions.tenant[DOCUMENT_EXTENSION_NAME].value = {
            documentName: decodeURIComponent(uploadedDocument.name),
            link: uploadedDocument.url,
            delete_link: uploadedDocument.delete_url,
            date: uploadedDocument.date
        };

        contactCollection.update(contact, callback);
    }

    return {

        checkDocumentContacts: function () {
            readDocumentDir(function (err, files, dir, nextDir) {
	        files = _.rest(files, startNumber);

                async.forEachSeries(files, function (file, next) {
                    var customerNumber = getCustomerNumber(file);
                    findContacts(customerNumber, function (err, contacts) {
                        if (contacts && contacts[0]) 
                            log(" SUCCESS finding " + customerNumber + " - " + file + " " + contacts[0].displayName);
                        else 
                            log(" ERROR " + customerNumber + " - " + file + ': ' + err);
                        next();
                    })
                }, function () {
                    nextDir();
                })
            }, function () {
                log(" DONE");
            })
        },

        attachDocuments: function () {

            readDocumentDir(function (err, files, dir, nextDir) {
	        files = _.rest(files, startNumber);

                async.forEachSeries(files, function (file, next) {

                        var customerNumber = getCustomerNumber(file);
                        findContacts(customerNumber, function (err, records) {
                            if (err) {
                                log(" ERROR when looking up contact '" + customerNumber + "' :" + JSON.stringify(err));
                                return next();
                            } 

                            if (!records || !records.length || records.length < 1) {
                                log(" ERROR finding contact '" + customerNumber + "' :" + JSON.stringify(err));
                                return next();                                
                            }

                            uploadDocument(dir + "/" + file, function (err, documentObj) {
                                if (err) {
                                    log(" ERROR uploading file '" + DOCUMENTS_DIR + "/" + file + "' : " + JSON.stringify(err));
                                    return next();
                                } 

                                async.forEachSeries(records, function (record, cb) {
                                    updateContact(record, documentObj, function (err) {
                                        if (err) {
                                            log(" ERROR updating contact:" + JSON.stringify(err));
                                        } else {
                                            log(" SUCCESS '" + record.displayName + "' attached with '" + DOCUMENTS_DIR + "/" + file + "'");
                                        }
                                        cb();
                                    });
                                }, function () {
                                    next();
                                });
                                
                            });
                        });
                    },
                    function () {
                        nextDir();
                    }
                )
            }, function () {
                log(" DONE");
            })
        }
    }

}
