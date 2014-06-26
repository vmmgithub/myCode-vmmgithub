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
    .option("-l, --line [s]", "Specify Start Line")
    .option("-t, --tenant [s]", "Scpecify Tenant");


commander
    .command("run")
    .description("execute dryRun")
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

    var findOpportunities = function (callback) {
        var start = commander.line ? parseInt(commander.line) : 0;
        var limit = 100;
        var opportunities = [];

        var findPart = function (callback) {
            oppCollection.find({}, {start: start, limit: limit}, function (err, records) {
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

    var addPartnerOpportunity = function (opportunity, callback) {
        delete opportunity.systemProperties;
        var partnerRels = _.filter(opportunity.relationships, function (rel) {
            return (rel.relation.name == "reseller" || rel.relation.name == "distributor");
        });

        var subordinateRels = _.filter(opportunity.relationships, function (rel) {
            return rel.relation.name == "subordinateOpportunity"
        });

        if (partnerRels.length && !subordinateRels.length) {
            var relationships = _.filter(opportunity.relationships,function(rel){
                return (rel.relation.name != "reseller" && rel.relation.name != "distributor");
            })
            var oppRecord = oppCollection.getRecord(opportunity._id);
            if (dryRun == false) {
                opportunity.relationships = relationships;
                oppCollection.update(opportunity,function(err,rec){
                    if(err){
                        console.log("Error updating opportunity '" + opportunity.displayName + "': " + JSON.stringify(err));
                        callback();
                    }  else {
                        oppRecord.execute('addChannelPartner', {
                            _id: opportunity._id,
                            canAddDistributor: true,
                            inversesummary: true,
                            relationships: partnerRels,
                            type : "app.channel.partner.input"
                        }, function(err,rec){
                            if(err){
                                console.log("Error 'addChannel partner for opp: '" + opportunity.displayName + "'" + JSON.stringify(err))
                            } else {
                                console.log("Success 'addChannel partner for opp: '" + opportunity.displayName + "'")
                            }
                            callback();
                        })
                    }
                })

            } else {
                console.log("DryRun Success partner opportunities created for '" + opportunity.displayName + "'");
                callback();
            }

        } else {
            console.log("No partner opportunities for '" + opportunity.displayName + "'");
            callback();
        }

    }


    return{
        run: function () {
            findOpportunities(function (err, records) {
                if (err) {
                    console.log("ERROR: " + JSON.stringify(err));
                } else if (records && records.length) {
                    console.log("FOUND: " + records.length + " Opportunities");
                    async.forEachSeries(records, addPartnerOpportunity, function () {
                        console.log("DONE");
                    })
                } else {
                    console.log("NO Records");
                }
            })
        }
    }
}
