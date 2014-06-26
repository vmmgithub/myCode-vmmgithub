#!/usr/bin/env node

var csvHelperInstance = require("./../lib/helpers/CsvHelper");
var async = require("async");
var commander = require("commander");
var $ = require("jquery");
var RestApiInterface = require('./../lib/helpers/RestApi');
var startLine = 0;


commander.version("1.0")
    .option("-h, --host [s]", "Specify host")
    .option("-s, --port [s]", "Specify Port")
    .option("-u, --user [s]", "Specify User")
    .option("-p, --password [s]", "Specify User Password")
    .option("-t, --tenant [s]", "Specify Tenant")
    .option("-f, --file [s]", "Specify File");

commander
    .command("batchQuarter")
    .description("execute batch quarter update")
    .action(function (cmd) {
        Scrub(true).executeBatchQuarterUpdate();
    });



commander
    .command("clientBatchQuarter")
    .description("execute client batch quarter update")
    .action(function (cmd) {
        Scrub(true).executeClientBatchQuarterUpdate();
    });



commander.parse(process.argv);


function Scrub(dryRun) {

    var restApi = new RestApiInterface(commander.host, commander.port, commander.user, commander.userPassword);
    restApi.setTenant(commander.tenant);

    var oppCollection = restApi.getCollection("app.opportunities", "app.opportunity");
    var csvHelper = new csvHelperInstance();

   
    var findOpportunity = function (displayName, callback) {
        oppCollection.find({
            displayName: displayName
        }, {
            start: 0,
            limit: 1
        }, callback);
    };

    var updateOppBatchQuarter = function (opportunity, batchQuarter, callback) {
        if (!opportunity.extensions) {
            opportunity.extensions = {};
        }
        if (!opportunity.extensions.master) {
            opportunity.extensions.master = {};
        }
        opportunity.extensions.master.batchQuarter = {
            type: "string"
        };

        {
            oppCollection.update(opportunity, callback);
        } 

    };

    var updateOppClientBatchQuarter = function (opportunity, clientBatchQuarter, callback) {
        if (!opportunity.extensions) {
            opportunity.extensions = {};
        }
        if (!opportunity.extensions.master) {
            opportunity.extensions.master = {};
        }
        opportunity.extensions.master.clientBatchQuarter = {
            type: "string"
        };

        {
            oppCollection.update(opportunity, callback);
        } 

    };

    return {

        executeClientBatchQuarterUpdate: function () {
            csvHelper.readAsObj(commander.file, function (data) {
                async.forEachSeries(data,
                    function (csvRecord, callback) {
                        findOpportunity(csvRecord["name"], function (err, opps) {
                              if (err || !opps || opps.length != 1) {
                                console.log("Error finding opportunity '" + oppName + "': " + JSON.stringify(err));
                                return callback();
                            }
                         if (opps && opps.length) {
                                updateOppClientBatchQuarter(opps[0],csvRecord["clientBatchQuarter"],function(err){
                                   if (err || !opps || opps.length != 1) {
                                console.log("Error finding opportunity '" + oppName + "': " + JSON.stringify(err));
                                return callback();
                                    } else {
                                        console.log("Success: Opportunity '" + oppName + "' successfully updated with '" + clientBatchQuarter + "' clientBatchQuarter");
                                    }
                                    callback();
                                })
                            } else {
                                console.log("Error: opportunity: '" + oppName + "' doesn't exist");
                                callback();
                            }
                        });
                    },
                    function () {
                        console.log("DONE");
                    }
                );
            });
        },

        executeBatchQuarterUpdate: function () {
            csvHelper.readAsObj(commander.file, function (data) {
                async.forEachSeries(data,
                    function (csvRecord, callback) {
                        findOpportunity(csvRecord["name"], function (err, opps) {
                            if (err || !opps || opps.length != 1) {
                                console.log("Error finding opportunity '" + oppName + "': " + JSON.stringify(err));
                                return callback();
                            } else if (opps && opps.length) {
                                updateOppBatchQuarter(opps[0],csvRecord["batchQuarter"],function(err){
                                    if (err || !opps || opps.length != 1) {
                                console.log("Error finding opportunity '" + oppName + "': " + JSON.stringify(err));
                                return callback();
                                    } else {
                                        var msg = "Success: Opportunity '" + oppName + "' successfully updated with '" + batchQuarter + "' batchQuarter";
                                        console.log(msg);
                                    }
                                    callback();
                                });
                            } else {
                                console.log("Error: opportunity: '" + oppName + "' doesn't exist");
                                callback();
                            }
                        });
                    },
                    function () {
                        console.log("DONE");
                    }
                );
            });
        }
    };
}
