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


var Scrub = function(){

    var dryRun = true;
    var start = 0;
    var limit = 50;
    var totalOpportunities = 0;
    var updatedOpportunities = 0;


    var findOpportunities = function(callback){
        oppHelper.find({"relationships.customer.targets.extensions.tenant.buId.value":"5455"}, { start:start,limit:limit,include:['_id', 'relationships',"extensions"]},function(err,opportunities){
            if(err){
                callback(err,null)
            } else if(opportunities && opportunities.length){
                callback(null,opportunities);
            } else{
                callback(null,null)
            }
        });
    }

    var geCustomerOppMap = function(opportunities){
        var oppCustomerIds = {};
        _.each(opportunities,function(opportunity){
            var rels = relationshipHelper.getRelationship(opportunity,"customer");
            if(rels.length){
                if(rels[0].target.key){
                    oppCustomerIds[rels[0].target.key] = opportunity;
                }else{
                    console.log("Error: Opportunity '" + opportunity.displayName + "' has no customer");
                }
            }
        });
        return oppCustomerIds;
    }

    var findCustomers = function(opportunities,callback){
        var customerOppMap = geCustomerOppMap(opportunities);
        //console.log( _.keys(customerOppMap));
        contactHelper.find({"_id": {"$in": _.keys(customerOppMap)}},{ start:0,limit:50,include:['_id', 'relationships',"extensions"]},function(err,customers){
            if(err){
                callback(err,null)
            } else if(customers && customers.length){
                callback(null,customers,opportunities,customerOppMap);
            }
        })
    }

    var checkOppCountryExtension = function(customers,opportunities,customerOppMap,callback){
        var oppToUpdate = [];
        _.each(customers,function(customer){
            var customerCountryExtension = extensionHelper.getExtension(customer,"tenant","country");
            var opportunity = customerOppMap[customer._id];
            var oppCountryExtension = extensionHelper.getExtension(opportunity,"master","country");
            if(oppCountryExtension && customerCountryExtension){
                if(customerCountryExtension.value.key != oppCountryExtension.value.key){
                    opportunity.extensions.master.country = customerCountryExtension;
                    oppToUpdate.push(opportunity);
                }
            } else if(customerCountryExtension && !oppCountryExtension){
                opportunity.extensions.master.country = customerCountryExtension;
                oppToUpdate.push(opportunity);
            } else{
                console.log("Error: Opp '" + opportunity.displayName + "' country extension could not be updated as the customer " + customer.displayName + " country extension is undefined");
            }

        });
        callback(null,oppToUpdate,opportunities);
    }

    var updateOpportunities = function(oppToUpdate,oppotunities,callback){
        var funcs = [];
        var updateOpp = function(opportunity){
            return function(callback){
                if(dryRun == false){
                    oppHelper.update(opportunity._id,opportunity,function(err,updatedRecord){
                        if(err){
                            console.log("Error: Opp '" + opportunity.displayName + "' could not be updated due error: " + JSON.stringify(err));
                            callback();
                        }else{
                            console.log("Success: Opportunity '" + opportunity.displayName + "' updated with country " + opportunity.extensions.master.country.value.displayName);
                            callback();
                        }
                    })
                } else{
                    console.log("Success: Dry Run Opportunity '" + opportunity.displayName + "' updated with country " + opportunity.extensions.master.country.value.displayName);
                    callback();
                }
            }
        }
        _.each(oppToUpdate,function(opportunity){
            funcs.push(new updateOpp(opportunity));
        });
        async.series(funcs,function(err,result){
            totalOpportunities += oppotunities.length;
            updatedOpportunities += oppToUpdate.length;
            callback(oppotunities);
        });

    }


    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            var checkResult = function(opportunities){
                if(opportunities.length && (opportunities.length == 50)){
                    start+=50;
                    executePart(checkResult);
                }else{
                    console.log("Done");
                    console.log("Total Opportunities: " + totalOpportunities);
                    console.log("Updated Opportunities: " + updatedOpportunities);
                }

            }
            var executePart = function(next){
                async.waterfall([
                    findOpportunities,
                    function(opportunities,callback){
                        findCustomers(opportunities,callback)
                    },
                    function(customers,opportunities,customerOppMap,callback){
                        checkOppCountryExtension(customers,opportunities,customerOppMap,callback)
                    },
                    function(oppToUpdate,opportunities,callback){
                        updateOpportunities(oppToUpdate,opportunities,callback)
                    }
                ],function(result){
                   next(result);
                })
            }
            executePart(checkResult);
        }
    }
}

new Scrub().execute(false);