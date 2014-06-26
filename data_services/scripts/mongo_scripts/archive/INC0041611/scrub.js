var API = require('./../lib/helpers/api'),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    IBMTenant = api.getTenant('ibm');
var collectionHelper = new require("./../lib/helpers/Collection");
var opportunityCollection = new collectionHelper(IBMTenant,"app.opportunities","app.opportunity");
var relationshipHelper = new require("./../lib/helpers/RelationshipHelper")();
var _ = require("underscore");
var async = require("async");

var Scrub = function(){

    var dryRun = true;

    var start = 0;
    var limit = 50;
    var totalOpportunities = 0;
    var processedOpportunities = 0;

    var change = {
        "incumbentReseller" : "reseller",
        "incumbentDistributor" : "distributor"
    }

    var findOpportunities = function(callback){
        //console.log("Find Opportunities");
        opportunityCollection.find({},{start:160,limit:1},function(err,records){
            callback(err,records);
        })
    }

    var updateOpportunity = function(opp,callback){
        if(opp && opp.relationships && opp.relationships.length){
            var changed = [];
            _.each(opp.relationships,function(rel,index){
                if(rel && rel.relation && rel.relation.name && change[rel.relation.name]){
                    //changed.push(change[rel.relation.name]);
                    rel.relation.name = change[rel.relation.name];
                    opp.relationships[index] = rel;
                }
            });
            //console.log(change["incumbentReseller"]);
            if(dryRun == false){
                opportunityCollection.update(opp,function(err){
                    if(err){
                        console.log("Error: can't update opportunity due error:" + JSON.stringify(err,null,4));
                    }else{
                        console.log("Success: opportunity '" + opp.displayName + "' updated with relationships: " + changed.join(","));
                    }
                    processedOpportunities+=1;
                    callback()
                })
            }else{
                console.log("Dry Run Success: opportunity '" + opp.displayName + "' updated with relationships: " + changed.join(","));
                processedOpportunities+=1;
                callback();
            }
        }else{
            callback();
        }

    }

    var updateOpportunities = function(opportunities,callback){
        if(opportunities && opportunities.length){
            async.forEachSeries(opportunities,function(record,callback){
                updateOpportunity(record,callback);
            },function(){
                callback();
            })
        }else{
            callback();
        }
    }



    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            var checkResults = function(err,records){
                if(err){
                    console.log("Error: can't get opportunities due error - " + JSON.stringify(err,null,4));
                    return;
                }
                if(records.length == limit){
                    totalOpportunities += records.length;
                    updateOpportunities(records,function(){
                        start+=50;
                        findOpportunities(checkResults);
                    });
                }else{
                    updateOpportunities(records,function(){
                        totalOpportunities += records.length;
                        console.log("DONE");
                        console.log("Total Opportunities:" + totalOpportunities);
                        console.log("Processed Opportunities:" + processedOpportunities);
                    })
                }
            }
            findOpportunities(checkResults);
        }

    }
}

new Scrub().execute();
