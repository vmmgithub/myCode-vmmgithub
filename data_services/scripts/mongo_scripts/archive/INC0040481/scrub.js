var API = require('./../lib/helpers/api'),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    IBMTenant = api.getTenant('ibm');
var csvHelper = new require("./../lib/helpers/CsvHelper")();
var collectionHelper = new require("./../lib/helpers/Collection");
var lookupCollection = new collectionHelper(IBMTenant,"core.lookups","core.lookup");
var contactCollection = new collectionHelper(IBMTenant,"core.contacts","core.contact");
var opportunityCollection = new collectionHelper(IBMTenant,"app.opportunities","app.opportunity");
var relationshipHelper = new require("./../lib/helpers/RelationshipHelper")();
var _ = require("underscore");
var async = require("async");
var fs = require("fs");

var Scrub = function(data){

    var dryRun = true;

    //R. Fricker
    //Ruedi Fricker

    var findContact = function(displayName,callback){
        contactCollection.find({displayName:displayName},{limit:1},function(err,contacts){
            if(err){
                callback(err);
            }else{
                if(contacts && contacts.length){
                    callback(null,contacts[0]);
                }else{
                    console.log("Error: Can't found salesRep: '" + displayName + "'");
                    callback(null,null);
                }
            }
        })
    }

    var findLookup = function(callback){
        lookupCollection.find({ 'group':'RelationshipType', 'name':'salesRep'}, {limit:1},function(err,lookups){
            if(err){
                callback(err);
            }else{
                if(lookups && lookups.length){
                    callback(null,lookups[0]);
                }else{
                    console.log("Error: Can't found Lookup: 'salesRep' ");
                    callback(null,null);
                }
            }
        })
    }

    var findOpportunity = function(displayName,callback){
        opportunityCollection.find({displayName:displayName},{limit:1},function(err,opportunities){
            if(err){
                callback(err);
            }else{
                if(opportunities && opportunities.length){
                    callback(null,opportunities[0]);
                }else{
                    console.log("Error: Can't found opportunity: '" + displayName + "'");
                    callback(null,null);
                }
            }
        })
    }

    var updateOpportunity = function(lookup,oldSalesRep,newSalesRep,opp,callback){
        _.each(opp.relationships,function(rel,index){
            if(rel.target.displayName == oldSalesRep.displayName){
                opp.relationships[index] = relationshipHelper.buildRelationship(lookup,newSalesRep);
                //console.log(relationshipHelper.getRelationship(opp,"salesRep"));
            }
        });
        if(dryRun == false){
            opportunityCollection.update(opp,function(err){
                if(err){
                    console.log("Error can't update opportunity due error: " + JSON.stringify(err))
                }else{
                    console.log("Success: opportunity '" + opp.displayName + "' successfully updated with salesRep 'Ruedi Fricker'")
                }
                callback();
            })
        }else{
            console.log("Dry Run Success: opportunity '" + opp.displayName + "' successfully updated with salesRep 'Ruedi Fricker'");
            callback();
        }

    }



    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            async.series({
                lookup : findLookup,
                oldSalesRep : function(callback){
                    findContact("R. Fricker",callback);
                },
                newSalesRep : function(callback){
                    findContact("Ruedi Fricker",callback)
                }
            },function(err,results){
                if(results.oldSalesRep && results.newSalesRep && results.lookup){
                    async.forEachSeries(data,function(record,callback){
                        async.waterfall([
                            function(callback){
                                findOpportunity(record.Name,callback);
                            },
                            function(opp,callback){
                                updateOpportunity(results.lookup,results.oldSalesRep,results.newSalesRep,opp,callback);
                            }
                        ],function(){
                            callback();
                        })
                    },function(){
                        console.log("DONE");
                    })
                }
            });
        }

    }
}

csvHelper.readAsObj("./csv/app_opportunities.csv",function(data){
    new Scrub(data).execute(false);
})
