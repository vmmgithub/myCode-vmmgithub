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
    .description("Assigns the account manager for each opportunity")
    .action(function (cmd) {
        Scrub(true).executeClientContactUpdate();
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

    var updateOppClientContact = function (opportunity, clientContact, callback) {
        opportunity.relationships = _.reject(opportunity.relationships, function (rel) {
            return rel.relation && rel.relation.name == 'clientContact'
        });

        opportunity.relationships.push({
            target: {
                type: "core.contact/person",
                key: clientContact._id,
                displayName: clientContact.displayName
            },
            relation: {
                name: "clientContact"
            },
            type: "core.relationship"
        });
        oppCollection.update(opportunity, callback);
    };

    var processRecord = function (oppName, clientContact, callback) {

        findOpportunity(oppName, function (err, opps) {
            if (err || !opps || opps.length != 1) {
                console.log("Error finding opportunity '" + oppName + "': " + JSON.stringify(err));
                return callback();
            }

            findContact(clientContact, function (err, contacts) {
                if (err || !contacts || contacts.length != 1) {
                    console.log("Error finding clientContact '" + clientContact + "': " + JSON.stringify(err));
                    return callback();
                }

                updateOppClientContact(opps[0], contacts[0], function (err) {
                    if (err) 
                        console.log("Error updating opportunity '" + oppName + "': " + JSON.stringify(err));
                    else 
                        console.log("Success: Opportunity '" + oppName + "' successfully updated with '" + clientContact + "' clientContact");

                    callback();
                });

            });

        });
    };

    return {
        executeClientContactUpdate: function () {
            console.log('Processing ' + commander.file);
            csvHelper.readAsObj(commander.file, function (data) {
                async.forEachSeries(data, function (csvRecord, callback) {
                        var oppName = csvRecord["Name"];
                        var clientContact = csvRecord["Client Contact"];

                        if (oppName && clientContact) {
                            processRecord(oppName, clientContact, callback);
                        } else {
                            console.log('Skipping ' + oppName + ' and ' + clientContact);
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
