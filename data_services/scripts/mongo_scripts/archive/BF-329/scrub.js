#!/usr/bin/env node

var csvHelperInstance = require("./../lib/helpers/CsvHelper");
var async = require("async");
var commander = require("commander");
var $ = require("jquery");
var RestApiInterface = require('./../lib/helpers/RestApi');
var startLine = 0;


commander.version("1.0")
    .option("-h, --host [s]", "Specify Host")
    .option("-p, --port [s]", "Specify Port")
    .option("-u, --user [s]", "Specify User")
    .option("-up, --user-password [s]", "Specify User Password")
    .option("-t, --tenant [s]", "Specify Tenant")
    .option("-f, --file [s]", "Specify File");


commander
    .command("batchQuarter")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(false).executeBatchQuarterUpdate();
    });

commander
    .command("dryRunBatchQuarter")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(true).executeBatchQuarterUpdate();
    });


commander
    .command("clientBatchQuarter")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(false).executeClientBatchQuarterUpdate()
    });

commander
    .command("dryRunClientBatchQuarter")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(true).executeClientBatchQuarterUpdate()
    });


commander.parse(process.argv);


function Scrub(dryRun) {

    var restApi = new RestApiInterface(commander.host, commander.port, commander.user, commander.userPassword);
    restApi.setTenant(commander.tenant);

    var oppCollection = restApi.getCollection("app.opportunities", "app.opportunity");
    var csvHelper = new csvHelperInstance();

    var findOpportunity = function (displayName, callback) {
        oppCollection.find({displayName: displayName}, {start: 0, limit: 1}, callback);
    }


    var updateOppBatchQuarter = function (opportunity, quarter, callback) {
        if (!opportunity.extensions) {
            opportunity.extensions = {}
        }
        if (!opportunity.extensions.master) {
            opportunity.extensions.master = {}
        }
        opportunity.extensions.master.batchQuarter = {
            type: "string",
            value: quarter
        }

        if (dryRun == false) {
            oppCollection.update(opportunity, callback);
        } else {
            callback()
        }

    }

    var updateOppClientBatchQuarter = function (opportunity, quarter, callback) {
        if (!opportunity.extensions) {
            opportunity.extensions = {}
        }
        if (!opportunity.extensions.master) {
            opportunity.extensions.master = {}
        }
        opportunity.extensions.master.clientBatchQuarter = {
            type: "string",
            value: quarter
        }

        if (dryRun == false) {
            oppCollection.update(opportunity, callback);
        } else {
            callback()
        }

    }

    return {

        executeClientBatchQuarterUpdate: function () {
            csvHelper.readAsObj(commander.file, function (data) {
                async.forEachSeries(data,
                    function (csvRecord, callback) {
                        findOpportunity(csvRecord["Name"], function (err, records) {
                            if (err) {
                                console.log("Error finding opportunity '" + csvRecord["Name"] + "': " + JSON.stringify(err));
                                callback();
                            } else if (records && records.length) {
                                updateOppClientBatchQuarter(records[0],csvRecord["Client Batch Quarter"],function(err){
                                    if(err) {
                                        console.log("Error updating opportunity '" + csvRecord["Name"] + "': " + JSON.stringify(err));

                                    } else {
                                        var msg = dryRun ? "DryRun " : "";
                                        msg += "Success: Opportunity '" + csvRecord["Name"] + "' successfully updated with '" + csvRecord["Client Batch Quarter"] + "' quarter";
                                        console.log(msg)
                                    }
                                    callback();
                                })
                            } else {
                                console.log("Error: opportunity: '" + csvRecord["Name"] + "' doesn't exists");
                                callback();
                            }
                        })
                    },
                    function () {
                        console.log("DONE");
                    }
                )
            })
        },

        executeBatchQuarterUpdate: function () {
            csvHelper.readAsObj(commander.file, function (data) {
                async.forEachSeries(data,
                    function (csvRecord, callback) {
                        findOpportunity(csvRecord["Name"], function (err, records) {
                            if (err) {
                                console.log("Error finding opportunity '" + csvRecord["Name"] + "': " + JSON.stringify(err));
                                callback();
                            } else if (records && records.length) {
                                updateOppBatchQuarter(records[0],csvRecord["Batch Quarter"],function(err){
                                    if(err) {
                                        console.log("Error updating opportunity '" + csvRecord["Name"] + "': " + JSON.stringify(err));

                                    } else {
                                        var msg = dryRun ? "DryRun " : "";
                                        msg += "Success: Opportunity '" + csvRecord["Name"] + "' successfully updated with '" + csvRecord["Batch Quarter"] + "' quarter";
                                        console.log(msg)
                                    }
                                    callback();
                                })
                            } else {
                                console.log("Error: opportunity: '" + csvRecord["Name"] + "' doesn't exists");
                                callback();
                            }
                        })
                    },
                    function () {
                        console.log("DONE");
                    }
                )
            })
        }


    }


}
