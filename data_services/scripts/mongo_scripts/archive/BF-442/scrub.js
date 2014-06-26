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
    .option("-s, --start [s]", "Scpecify Start Number")
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

commander
    .command("replace")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(true).replaceExtension();
    });


commander.parse(process.argv);

function Scrub(dryRun) {

    var extensionName = "country";

    var restApi = new RestApiInterface(commander.host, commander.port, commander.user, commander.userPassword);
    restApi.setTenant(commander.tenant);
    var oppCollection = restApi.getCollection("app.opportunities", "app.opportunity");
    var offerCollection = restApi.getCollection("app.offers", "app.offer");

    var findOpportunities = function (callback) {
        var start = commander.start == undefined ? 0 : parseInt(commander.start);
        var limit = 100;
        var opportunities = [];
        var findPart = function () {
            oppCollection.find({}, {start: start, limit: limit}, function (err, records) {
                if (err) {
                    callback(err)
                } else if (records && records.length) {
                    opportunities = opportunities.concat(records);
                    if (records.length == limit) {
                        start += limit;
                        findPart();
                    } else {
                        console.log("XXX:" + records.length);
                        callback(null, opportunities);
                    }
                } else {
                    console.log("YYY:" + records.length);
                    callback(null, opportunities);
                }
            })
        }
        findPart();

    }

    var findOffers = function (opportunity, callback) {
        oppCollection.getRecord(opportunity._id).execute("findOffers", {}, function (err, response) {
            var offers = null;
            if (response && response.data && response.data["app.offer"]) {
                offers = response.data["app.offer"];
            }
            callback(err, offers);
        });
    }

    var copyCountryExtension = function (opportunity, offers, next) {
        var checkOfferExtensions = function (offer) {
            if (!offer.extensions) {
                offer.extensions = {}
            }
            if (!offer.extensions.tenant) {
                offer.extensions = {}
            }
        }
        var updateList = [];
        var offerNames = [];
        if (opportunity.extensions && opportunity.extensions.master && opportunity.extensions.master[extensionName]) {
            var oppExtension = opportunity.extensions.master[extensionName];
            console.log("Copying opportunity '" + opportunity.displayName + "' country extension " + oppExtension.value.displayName);
            _.each(offers, function (offer) {
                checkOfferExtensions(offer);
                offer.extensions.tenant[extensionName] = oppExtension;
                offerNames.push(offer.displayName);
                updateList.push(function (callback) {
                    offerCollection.update(offer, callback);
                });
            });
        }
        console.log("Offers to update: " + offerNames.join(", "));
        if (dryRun == false) {
            async.series(updateList, next);
        } else {
            next();
        }
    }

    return{
        run: function () {
            findOpportunities(function (err, opportunities) {
                if (err) {
                    console.log("Error find opportunities: " + JSON.stringify(err))
                } else if (opportunities && opportunities.length) {
                    console.log("Found " + opportunities.length + " opportunities");
                    async.forEachSeries(opportunities, function (opportunity, next) {
                        findOffers(opportunity, function (err, offers) {
                            if (err) {
                                console.log("Error find offers for opportunity '" + opportunity.displayName + "' " + JSON.stringify(err));
                                next();
                            } else if (offers && offers.length) {
                                copyCountryExtension(opportunity, offers, next);

                            } else {
                                console.log("Found 0 offers for opportunity '" + opportunity.displayName + "'");
                                next();
                            }
                        })
                    }, function () {
                        console.log("DONE");
                    })
                } else {
                    console.log("Fount 0 opportunities");
                }
            })
        },

        replaceExtension: function () {
            var eaCollection = restApi.getCollection("core.extension.attributes", "core.extension.attribute");
            eaCollection.find({"model": "app.offer","systemProperties.tenant":"ibm"}, {}, function (err, records) {
                if (err) {
                    console.log("Error find core.extension.attributes: " + JSON.stringify(err));
                } else if (records && records.length){
                    var offerEA = records[0];
                    offerEA.extensions = _.filter(offerEA.extensions,function(ext){return ext.name != "country"});
//                    console.log(offerEA.extensions);
                    offerEA.extensions.push({
                        type : "core.extension.attribute",
                        fromMaster:false,
                        name : "country",
                        displayName : "Country",
                        cardinality:1,
                        attributeType : "lookup"
                    });
                    offerCollection.update(offerEA,function(err,doc){
                        console.log(doc)
                        console.log("Replaced");
                    })
                } else {
                    console.log("No records found");
                }
            })
        }

    }

}