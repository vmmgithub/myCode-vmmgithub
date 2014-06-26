var API = require('./../lib/helpers/api'),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    dellT = api.getTenant('dell');
var csvHelper = new require("./../lib/helpers/CsvHelper")(dellT);
var oppHelper = new require("./../lib/helpers/OpportunityHelper")(dellT);
var coreLookupHelper = new require("./../lib/helpers/CoreLookupHelper")(dellT);
var contactHelper = new require("./../lib/helpers/ContactHelper")(dellT);
var relationshipHelper = new require("./../lib/helpers/RelationshipHelper")();
var _ = require("underscore");
var async = require("async");
var fs = require("fs");


var Scrub = function(data){

    var lookup = null;
    var personsCache = {};
    var dryRun = true;


    var findLookup = function(callback){
        coreLookupHelper.find({ 'group':'RelationshipType', 'name':'salesRep'}, {limit:1},function(err,lookups){
            if(err){
                callback(err,null);
            }else if(lookups && lookups.length){
                lookup = lookups[0];
                callback(null,lookups[0]);
            }
        });
    }

    var findContact = function(personName,callback){
        if(!personsCache[personName]){
            contactHelper.find({displayName:personName}, {limit:1, include:['_id', 'type']},function(err,persons){
                if(err){
                    callback(err,null);
                }else if(persons && persons.length){
                    personsCache[personName] = persons[0];
                    callback(null,persons[0])
                }else{
                    callback(null,null)
                }
            })
        }else{
            callback(null,personsCache[personName]);
        }
    }

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


    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            findLookup(function(err,lookup){
                async.forEachSeries(data,function(record,callback){
                    async.series({
                        salesRep : function(callback){
                            findContact(record.salesRep,callback)
                        },
                        opportunity : function(callback){
                            findOpportunity(record.opportunity,callback);
                        }
                    },function(err,results){
                        if(err){
                            console.log("Error: Opp '" + record.opportunity + "' could not be updated as the sales rep " + record.salesRep + " due error: " + JSON.stringify(err));
                        } else{
                            if(!results.salesRep){
                                console.log("Error: Opp '" + record.opportunity + "' could not be updated as the sales rep '" + record.salesRep + "' could not be found.");
                                callback();
                            } else if(!results.opportunity){
                                console.log("Error: Opp " + record.opportunity + " could not be updated as opportunity could not be found");
                                callback();
                            } else if(results.salesRep && results.opportunity){
                                var relationship = relationshipHelper.buildRelationship(lookup,results.salesRep);
                                var prevSalesRepRelationships = relationshipHelper.getRelationship(results.opportunity,"salesRep");
                                fs.appendFileSync("backup/burnell_to_fix.log",record.opportunity + ": " + JSON.stringify(prevSalesRepRelationships,null,4));
                                fs.appendFileSync("backup/burnell_to_fix.log","\n");
                                relationshipHelper.replaceRelationship(results.opportunity,relationship);
                                if(dryRun == false){
                                    oppHelper.update(results.opportunity._id,results.opportunity,function(err,updatedRecord){
                                        if(err){
                                            console.log("Error: Opp '" + record.opportunity + "' could not be updated as the sales rep " + record.salesRep + " due error: " + JSON.stringify(err));
                                            callback();
                                        }else{
                                            console.log("Success: " + record.opportunity + " updated with salesRep " + record.salesRep);
                                            callback();
                                        }
                                    })
                                }else{
                                    console.log("DryRun Success: " + record.opportunity + " updated with salesRep " + record.salesRep);
                                    callback();
                                }
                            }
                        }
                        //console.log("Sales Rep",results.salesRep.displayName);
                        //console.log("Opportunity",results.opportunity.displayName);

                    })
                })
            })
        }
    }
}

//csvHelper.readAsObj("./csv/Burnell_UTF-8.csv",function(data){
//    console.log(data);
//})
csvHelper.readAsObj("./csv/tofix/Burnel_TO_FIX.csv",function(data){
    new Scrub(data).execute(false);
})