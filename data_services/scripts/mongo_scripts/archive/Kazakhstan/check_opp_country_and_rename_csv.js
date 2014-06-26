var API = require('./../lib/helpers/api'),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    dellT = api.getTenant('dell');
var csvHelper = new require("./../lib/helpers/CsvHelper")(dellT);
var oppHelper = new require("./../lib/helpers/OpportunityHelper")(dellT);
var contactHelper = new require("./../lib/helpers/ContactHelper")(dellT);
var extensionHelper = new require("./../lib/helpers/ExtensionHelper");
var relationshipHelper = new require("./../lib/helpers/RelationshipHelper")();
var _ = require("underscore");
var async = require("async");
var fs = require("fs");


var Scrub = function(list){

    var dryRun = true;
    var oppNames = list;


    var findOpportunity = function(oppName,callback){
        oppHelper.find({ displayName:oppName}, { limit:1, include:['_id', 'relationships']},function(err,opp){
            if(err){
                console.log("Error: can't found opportunity by name '" + oppName + "'");
                callback(err,null)
            } else if(opp && opp.length){
                callback(null,opp[0]);
            } else{
                console.log("Error: can't found opportunity by name '" + oppName + "'");
                callback(null,null)
            }
        });
    }

    var findCustomer = function(opportunity,callback){
        var customerRelationship = relationshipHelper.getRelationship(opportunity,"customer");
        if(customerRelationship && customerRelationship.length && customerRelationship[0].target && customerRelationship[0].target.key){
            contactHelper.find({"_id": customerRelationship[0].target.key},{},function(err,customers){
                if(err){
                    console.log("Error: Opp '" + opportunity.displayName + "' can't found customer : " + customerRelationship[0].target.displayName + "due error " + JSON.stringify(err));
                    callback(null,null,null);
                }else{
                    if(customers && customers.length){
                        callback(null,opportunity,customers[0]);
                    }else{
                        console.log("Error Opp '" + opportunity.displayName + "' can't found customer : " + customerRelationship[0].target.displayName);
                        callback(null,null,null);
                    }
                }
            })
        } else{
            console.log("Error: Opportunity '" + opportunity.displayName + "' has no customer");
            callback(null,null,null);
        }
    }



    var checkOppCountryExtension = function(customer,opportunity,callback){
        var customerCountryExtension = extensionHelper.getExtension(customer,"tenant","country");
        var oppCountryExtension = extensionHelper.getExtension(opportunity,"master","country");
        var updateOpportunity = function(opp,contryExtension){
            var oldDisplayName = opportunity.displayName;
            opportunity.displayName = opportunity.displayName.replace(/_KZ/g,"_" + contryExtension.value.name);
            if(dryRun == false){
                oppHelper.update(opp._id,opp,function(err){
                    if(err){
                        console.log("Error: Opp '" + opportunity.displayName + "' could not be updated due error: " + JSON.stringify(err));
                        callback();
                    }else{
                        console.log("Success: Opportunity '" + opportunity.displayName + "' updated with country " + opportunity.extensions.master.country.value.displayName + " and displayName from '" + oldDisplayName + "' to '" + opportunity.displayName + "'");
                        callback();
                    }
                })
            }else{
                console.log("Dry Run Success: Opportunity '" + opportunity.displayName + "' updated with country " + opportunity.extensions.master.country.value.displayName + " and displayName from '" + oldDisplayName + "' to '" + opportunity.displayName + "'");
                callback();
            }
        }

        if(oppCountryExtension && customerCountryExtension){
            if(customerCountryExtension.value.key != oppCountryExtension.value.key){
                opportunity.extensions.master.country = customerCountryExtension;
                updateOpportunity(opportunity,customerCountryExtension);
            }else{
                callback();
            }
        } else if(customerCountryExtension && !oppCountryExtension){
            opportunity.extensions.master.country = customerCountryExtension;
            updateOpportunity(opportunity,customerCountryExtension);
        } else{
            console.log("Error: Opp '" + opportunity.displayName + "' country extension could not be updated as the customer " + customer.displayName + " country extension is undefined");
            callback();
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
                    function(opportunity,callback){
                        if(opportunity){
                            findCustomer(opportunity,callback);
                        }else{
                            callback();
                        }
                    },
                    function(oportunity,customer,callback){
                        if(oportunity && customer){
                            checkOppCountryExtension(customer,oportunity,callback)
                        }else{
                            callback();
                        }
                    }
                ],function(){
                    callback();
                })
            });
        }
    }
}

csvHelper.readAsObj("./csv/opportunities.csv",function(data){
    new Scrub(data).execute(false);
})