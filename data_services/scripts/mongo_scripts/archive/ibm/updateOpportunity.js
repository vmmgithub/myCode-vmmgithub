#!/usr/bin/env node

var async = require("async");
var commander = require("commander");
var RestApiInterface = require('./../lib/helpers/RestApi');
var TeamHelper = require('./../lib/helpers/TeamHelper');
var RelHelper = require('./../lib/helpers/RelationshipHelper')();
var _ = require("underscore");
var fs = require("fs");


commander.version("1.0")
    .option("-h, --host [s]", "Scpecify Host")
    .option("-p, --port [s]", "Scpecify Port")
    .option("-u, --user [s]", "Scpecify User")
    .option("-up, --user-password [s]", "Scpecify User Password")
    .option("-t, --tenant [s]", "Scpecify Tenant");

commander
    .command("update")
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(true).update();
    });


commander.parse(process.argv);


function Scrub(dryRun){

    var restApi = new RestApiInterface(commander.host,commander.port,commander.user,commander.userPassword);
    var tenant = restApi.setTenant(commander.tenant);
    var oppCollection = restApi.getCollection("app.opportunities","app.opportunity");


    var findOpportunities = function(callback){
        var start = 0;
        var limit = 50;
        var opportunities = [];
        var findPart = function(){
            var filter = {};
            oppCollection.find(filter,{start:start,limit:limit},function(err,records){
                if(err){
                    callback(err);
                }else{
                    if(records && records.length){
                        opportunities = opportunities.concat(records);
                        if(records.length != limit){
                            callback(null,opportunities);
                        }else{
                            start+=limit;
                            findPart();
                        }

                    }else{
                        callback(null,opportunities);
                    }
                }
            });
        }
        findPart();
    }

    return{
        update : function(){
            findOpportunities(function(err,opportunities) {
                if(err){
                    console.log("ERROR: " + JSON.stringify(err));
                } else if(opportunities && opportunities.length) {
                    console.log("Found: " + opportunities.length + " opportunities");
                    async.forEachSeries(opportunities,function(opp,next){
                        RelHelper.removeRelationships(opp,"assignedTeam");
                        oppCollection.update(opp,function(err){
                            if(err){
                                console.log("ERROR updating Opportunity '" + opp.displayName + "': " + JSON.stringify(err));
                            } else {
                                console.log("Success Opportunity '" + opp.displayName + "' successfully updated");
                            }
                            next();
                        })
                    },function(){
                        console.log("DONE");
                    })
                } else {
                    console.log("No Opportunities found");
                }
            })
        }
    }


}