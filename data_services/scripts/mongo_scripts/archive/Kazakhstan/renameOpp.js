var API = require('./../lib/helpers/api'),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    dellT = api.getTenant('dell');
var csvHelper = new require("./../lib/helpers/CsvHelper")(dellT);
var oppHelper = new require("./../lib/helpers/OpportunityHelper")(dellT);
var coreLookupHelper = new require("./../lib/helpers/CoreLookupHelper")(dellT);
var contactHelper = new require("./../lib/helpers/ContactHelper")(dellT);
var relationshipHelper = new require("./../lib/helpers/RelationshipHelper")();
var extensionHelper = new require("./../lib/helpers/ExtensionHelper");
var _ = require("underscore");
var async = require("async");
var fs = require("fs");


var Scrub = function(data){


    var dryRun = true;
    var oppNames = data;

    var findOpportunity = function(oppName,callback){
        oppHelper.find({ displayName:oppName}, { limit:1, include:['_id', 'relationships']},function(err,opp){
            if(err){
                callback(err,null)
            } else if(opp && opp.length){
                callback(null,opp[0]);
            } else{
                callback(null,null)
            }
        });
    }

    var updateOppName = function(opportunity,callback){
        var contryExtension = extensionHelper.getExtension(opportunity,"master","country");
        if(!contryExtension){
            console.log("Error: Opportunity '" + opportunity.displayName + "' has not extension country");
            callback();
        }else{
            var oldDisplayName = opportunity.displayName;
            opportunity.displayName = opportunity.displayName.replace(/_KZ/g,"_" + contryExtension.value.name);
            if(dryRun == false){
                oppHelper.update(opportunity._id,opportunity,function(err,updatedRecord){
                    if(err){
                        console.log("Error: Opp '" + opportunity.displayName + "' could not be updated due error: " + JSON.stringify(err));
                        callback();
                    }else{
                        console.log("Success: Opportunity '" + oldDisplayName + "' updated with displayName '" + opportunity.displayName + "'");
                        callback();
                    }
                })
            }else{
                console.log("Dry Run Success: Opportunity '" + oldDisplayName + "' updated with displayName '" + opportunity.displayName + "'");
                callback();
            }
        }

    }


    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            async.forEachSeries(oppNames,function(record,callback){
                async.waterfall([
                    function(callback){
                        findOpportunity(record.Name,callback)
                    },
                    function(opportunity,callaback){
                        updateOppName(opportunity,callaback)
                    }
                ],function(){
                    callback();
                })
            });
        }
    }
}


csvHelper.readAsObj("./csv/updated_left.csv",function(data){
    new Scrub(data).execute(false);
})