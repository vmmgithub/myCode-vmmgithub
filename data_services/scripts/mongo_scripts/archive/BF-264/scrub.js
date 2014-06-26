#!/usr/bin/env node

var csvHelperInstance = require("./../lib/helpers/CsvHelper");
var async = require("async");
var commander = require("commander");
var RestApiInterface = require('./../lib/helpers/RestApi');
var RelHelper = require('./../lib/helpers/RelationshipHelper')();
var _ = require("underscore");


commander.version("1.0")
    .option("-h, --host [s]", "Scpecify Host")
    .option("-p, --port [s]", "Scpecify Port")
    .option("-u, --user [s]", "Scpecify User")
    .option("-up, --user-password [s]", "Scpecify User Password")
    .option("-t, --tenant [s]", "Scpecify Tenant");


commander
    .command("run")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(false).run();
    });

commander
    .command("dryRun")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(true).run();
    });

commander.parse(process.argv);

function Scrub(dryRun) {

    var restApi = new RestApiInterface(commander.host, commander.port, commander.user, commander.userPassword);
    var tenant = restApi.setTenant(commander.tenant);

    var oppCollection = restApi.getCollection("app.opportunities", "app.opportunity");
    var lookupCollection = restApi.getCollection("app.lookups", "app.lookup");


    var checkSellingPeriod = function (opp, periods) {
        var res = null;
        var targetDate = new Date(opp.targetDate);
        _.each(periods, function (lookup, key) {
            var val = lookup.value;
            var start = new Date(val.start);
            var end = new Date(val.end);
            if ((targetDate.getTime() >= start.getTime()) && (targetDate.getTime() < end.getTime())) {
                res = lookup;
                return false;
            }
        });
        return res;
    }

    function findSellingPeriods(callback) {
        lookupCollection.find({ 'group': 'TargetSelling'}, {}, function (err, records) {
            if (err) {
                callback(err, null);
            } else if (records && records.length) {
                callback(null, records);
            } else {
                callback(null, null);
            }
        })
    }

    function findOpportunities(callback) {

        var start = 0;
        var limit = 50;
        var opportunities = [];

        var findPart = function () {
            oppCollection.find({}, {start: start, limit: limit}, function (err, records) {
                if (err) {
                    console.log("Error found opportunities: " + JSON.stringify(err));
                    callback(err, opportunities);
                } else if (records && records.length) {
                    opportunities = opportunities.concat(records);
                    if (records.length == limit) {
                        start += limit;
                        findPart();
                    } else {
                        callback(null, opportunities);
                    }
                } else {
                    callback(null, opportunities);
                }
            })
        }

        findPart();
    }


    return{
        run: function () {
            findSellingPeriods(function (err, lookups) {
                if (err) {
                    console.log("Error getting lookups: " + JSON.stringify(err));
                } else {
                    console.log("FOUND " + lookups.length + " lookups");
                    findOpportunities(function (err, opportunities) {
                        if (err) {
                            console.log("Error getting opportunities: " + JSON.stringify(err));
                        } else {
                            console.log("FOUND " + opportunities.length + " opportunities");
                            async.forEachSeries(
                                opportunities,
                                function (opp, next) {
                                    var sellingPeriod = checkSellingPeriod(opp, lookups);
                                    if (sellingPeriod && (sellingPeriod.name != opp.extensions.master.targetPeriod.value.name)) {
                                        opp.extensions.master.targetPeriod.value = sellingPeriod;
                                        oppCollection.update(opp, function (err) {
                                            if (err) {
                                                console.log("Error updating opportunity '" + opp.displayName + "': " + JSON.stringify(err));
                                            } else {
                                                console.log("Success updating opportunity '" + opp.displayName + "'");
                                            }
                                            next();
                                        })
                                    } else {
                                        console.log("Error opportunity '" + opp.displayName + "' invalid target date " + opp.targetDate);
                                        next();
                                    }

                                },
                                function () {
                                    console.log("DONE");
                                }
                            )
                        }
                    })
                }
            })
        }
    }


}
