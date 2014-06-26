#!/usr/bin/env node

var async = require("async");
var commander = require("commander");
var RestApiInterface = require('./../lib/helpers/RestApi');
var _ = require("underscore");


commander.version("1.0")
    .option("-h, --host [s]", "Scpecify Host")
    .option("-p, --port [s]", "Scpecify Port")
    .option("-u, --user [s]", "Scpecify User")
    .option("-up, --user-password [s]", "Scpecify User Password")
    .option("-t, --tenant [s]", "Scpecify Tenant");


commander
    .command("run")
    .description("execute")
    .action(function (cmd) {
        Scrub(false).run()
    });

commander
    .command("dryRun")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(true).run()
    });


commander.parse(process.argv);


function Scrub(dryRun) {

    var restApi = new RestApiInterface(commander.host, commander.port, commander.user, commander.userPassword);
    restApi.setTenant(commander.tenant);
    var oppCollection = restApi.getCollection("app.opportunities", "app.opportunity");
    var quoteCollection = restApi.getCollection("app.quotes", "app.quote");
    var appLookupCollection = restApi.getCollection("app.lookups", "app.lookup");


    var findLookup = function (callback) {
        appLookupCollection.find({group: "RequestReason"}, {}, function (err, records) {
            if (err) {
                callback(err)
            } else if (records && records.length) {
                var filtered = _.filter(records, function (item) {
                    return (item.name == "initialQuote")
                });
                if (filtered && filtered.length) {
                    callback(null, filtered[0]);
                }
            } else {
                callback();
            }
        });
    }

    var findOpportunities = function (callback) {
        var start = 0;
        var limit = 100;
        var opportunities = [];

        var findPart = function (callback) {
            var filter = {"isSubordinate": false, "$or": [
                {"relationships.quote": {"$nin": [null]}},
                {"relationships.primaryQuote": {"$nin": [null]}}
            ] };
            oppCollection.find(filter, {start: start, limit: limit}, function (err, records) {
                if (err) {
                    callback(err);
                } else {
                    if (records && records.length) {
                        opportunities = opportunities.concat(records);
                        if (records.length == limit) {
                            start += limit;
                            findPart(callback);
                        } else {
                            callback(null, opportunities);
                        }
                    }
                }
            });
        }

        findPart(callback);
    }

    var findQuote = function (opportunity, callback) {
        var quoteId = null;
        _.each(opportunity.relationships, function (rel) {
            if (rel.relation.name == "latestQuote") {
                quoteId = rel.target.key;
            } else if ((rel.relation.name == "primaryQuote")) {
                quoteId = rel.target.key;
            } else if ((rel.relation.name == "quote") && (quoteId == null)) {
                quoteId = rel.target.key;
            }
        });

        console.log(quoteId);

        if (quoteId != null) {
            quoteCollection.find({"_id": quoteId}, {}, function (err, records) {
                if (err) {
                    callback(err)
                } else if (records && records.length) {
                    callback(null, records[0]);
                } else {
                    callback();
                }
            })
        } else {
            callback();
        }

    }


    var createResellerQuote = function (opportunity, quote, lookup, next) {
        var subordinateRels = _.filter(opportunity.relationships, function (rel) {
            return rel.relation.name == "subordinateOpportunity"
        });
        if (subordinateRels.length) {
            var _ids = _.map(subordinateRels, function (item) {
                return item.target.key;
            });
            console.log(_ids);
            var filter = {
                "_id": {"$in": _ids}
            }
            var oppRecord = oppCollection.getRecord(opportunity._id);
            oppRecord.execute("findPartnerOpps", {
                filter: filter
            }, function (err, res) {
                console.log(res);
                if (res && res.data && res.data["app.opportunity"] && res.data["app.opportunity"].length) {
                    async.forEachSeries(res.data["app.opportunity"], function (record, nextOpp) {
                        var subOpp = oppCollection.getRecord(record._id);
                        subOpp.execute("getQuoteInput", {
                            _id: opportunity._id,
                            displayName: opportunity.displayName,
                            type: "app.opportunity"

                        }, function (err, res) {
                            var quoteInput = res && res.data && res.data["app.quote.input"] && res.data["app.quote.input"][0];
                            if(err){
                                console.log("Error getting quoteInput: " + JSON.stringify(err));
                                nextOpp();
                            } else if (quoteInput) {
                                quoteInput.requestReason = lookup;
                                quoteInput.requestedCurrency = quote.amount.code;
                                quoteInput.dueDate = quote.dueDate;
                                if (dryRun == false) {
                                    subOpp.execute("requestQuote", quoteInput, function(err,rec){
                                        if(err){
                                            console.log("Error requesting quote for opp '" + record.displayName + "' :" + JSON.stringify(err));
                                        } else {
                                            console.log("Success requesting quote for opp '" + record.displayName + "'");
                                        }
                                        nextOpp();
                                    });
                                } else {
                                    console.log("DryRun Success creating quote");
                                    nextOpp();
                                }

                            } else {
                                console.log("Can't get quoteInput for opp '" + record.displayName + "'");
                                nextOpp();
                            }
                        })
                    }, function () {
                        next();
                    })

                } else {
                    console.log("No partner opportunities for opp '" + opportunity.displayName + "'");
                    next();
                }

            })
        } else {
            console.log("No partner opportunities for opp '" + opportunity.displayName + "'");
            next();
        }
    }


    return{
        run: function () {
            findLookup(function (err, lookup) {
                if (err) {
                    console.log("ERROR: " + JSON.stringify(err));
                } else {
                    findOpportunities(function (err, records) {
                        console.log("FOUND: " + records.length);
                        if (err) {
                            console.log("ERROR: " + JSON.stringify(err));
                        } else if (records && records.length) {
                            async.forEachSeries(records, function (record, next) {
                                findQuote(record, function (err, quote) {
                                    if (err) {
                                        console.log("ERROR: " + JSON.stringify(err));
                                        next();
                                    } else if (quote) {
                                        createResellerQuote(record, quote, lookup, next)
                                    } else {
                                        console.log("No quote for opp: '" + record.displayName + "'");
                                        next();
                                    }
                                })
                            }, function () {
                                console.log("DONE");
                            })
                        }
                    })
                }
            });
        }
    }
}
