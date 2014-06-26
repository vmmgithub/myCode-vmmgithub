#!/usr/bin/env node

var async = require("async");
var _ = require("underscore");
var commander = require("commander");
var csvHelperInstance = require("./../lib/helpers/CsvHelper");
var RestApiInterface = require('./../lib/helpers/RestApi');

commander.version("1.0")
    .option("-h, --host [s]", "Specify host")
    .option("-s, --port [s]", "Specify Port")
    .option("-u, --user [s]", "Specify User")
    .option("-p, --password [s]", "Specify User Password")
    .option("-t, --tenant [s]", "Specify Tenant")
    .option("-f, --file [s]", "Specify File");

commander
    .command("assign")
    .description("Assigns the sales rep for each opportunity")
    .action(function (cmd) {
        Scrub(true).executeSalesRepUpdate();
    });

commander.parse(process.argv);

function Scrub() {

    var restApi = new RestApiInterface(commander.host, commander.port, commander.user, commander.password);
    restApi.setTenant(commander.tenant);

    var oppCollection = restApi.getCollection("app.opportunities", "app.opportunity");
    var contactCollection = restApi.getCollection("core.contacts", "core.contact");
    var csvHelper = new csvHelperInstance();

    var findOpportunity = function (displayName, callback) {
        oppCollection.find({
            displayName: displayName
        }, {
            start: 0,
            limit: 1
        }, callback);
    };

    var findContact = function (displayName, callback) {
        contactCollection.find({
            displayName: displayName
        }, {
            start: 0,
            limit: 1
        }, callback);
    };

    var updateOppSalesRep = function (opportunity, salesRep, callback) {
        opportunity.relationships.push({
            target: {
                type: "core.contact/person",
                key: salesRep._id,
                displayName: salesRep.displayName
            },
            relation: {
                name: "salesRep"
            },
            type: "core.relationship"
        });
        oppCollection.update(opportunity, callback);
    };

    var processRecord = function (oppName, salesRep, callback) {

        findOpportunity(oppName, function (err, opps) {
            if (err || !opps || opps.length != 1) {
                console.log("Error finding opportunity '" + oppName + "': " + JSON.stringify(err));
                return callback();
            }

            findContact(salesRep, function (err, contacts) {
                if (err || !contacts || contacts.length != 1) {
                    console.log("Error finding salesRep '" + salesRep + "': " + JSON.stringify(err));
                    return callback();
                }

                updateOppSalesRep(opps[0], contacts[0], function (err) {
                    if (err) 
                        console.log("Error updating opportunity '" + oppName + "': " + JSON.stringify(err));
                    else 
                        console.log("Success: Opportunity '" + oppName + "' successfully updated with '" + salesRep + "' salesRep");

                    callback();
                });

            });

        });
    };

    return {
        executeSalesRepUpdate: function () {
            console.log('Processing ' + commander.file);
            csvHelper.readAsObj(commander.file, function (data) {
                async.forEachSeries(data, function (csvRecord, callback) {
                        var oppName = csvRecord["name"];
                        var salesRep = csvRecord["salesRep"];

                        if (oppName && salesRep) {
                            processRecord(oppName, salesRep, callback);
                        } else {
                            console.log('Skipping ' + oppName + ' and ' + salesRep);
                            callback();
                        }
                    },
                    function (err) {
                        console.log("DONE " + err);
                    });
            });
        }
    };
}
