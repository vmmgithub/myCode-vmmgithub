var API = require('./../lib/helpers/api'),
    Collection = require("./../lib/helpers/Collection"),
    async = require("async"),
    _ = require("underscore"),
    relHelper = new require("./../lib/helpers/RelationshipHelper")(),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    tenant = api.getTenant('ibm'),
    oppCollection = new Collection(tenant,"app.opportunities","app.opportunity"),
    offerCollection = new Collection(tenant,"app.offers","app.offer"),
    contactsCollection = new Collection(tenant,"core.contacts","core.contact"),
    assetCollection = new Collection(tenant,"app.assets","app.asset");


var Scrub = function(){

    var dryRun = true;
    var start = 0;
    var limit = 50;
    var totalOpportunities = 0;
    var updatedOpportunities = 0;
    var oppList = [];


    var findOpportunities = function(callback){

        var findPart = function(callback){
            var filter = {};
            //console.log(filter);
            oppCollection.find(filter, { start:start,limit:limit},function(err,opportunities){
                //console.log(err)
                //console.log(opportunities);
                if(opportunities && opportunities.length){
                    totalOpportunities += opportunities.length;
                }
                callback(opportunities)
            });
        }
        var checkResult = function(opportunities){
            if(opportunities && opportunities.length){
                opportunities.forEach(function(opp){
                    oppList.push(opp);
                })
            }
            if(!opportunities || (opportunities.length != limit)){
                callback();
            }else{
                //console.log("Start: " + start + " Limit: " + limit);
                start += limit;
                findPart(checkResult);
            }
        }
        findPart(checkResult);

    }


    var addProjection = function(callback){

        var buildExtensions = function(opp,customer){
            var extensionNames = ["clientID","customerNumber","parentGroup"];
            var extensions = {
                tenant : {}
            };
            var hasOppExtension = function(extensionName){
                var res = false;
                var customerRel = _.filter(opp.relationships,function(rel){
                    return (rel.relation.name == "customer");
                })[0];
                if(customerRel && customerRel.extensions && customerRel.extensions.tenant){
                    _.each(customerRel.extensions.tenant,function(extension,name){
                        if((name == extensionName) && extension.value){
                            res = true;
                            return false;
                        }
                    });
                }
                return res;
            }
            if(customer.extensions && customer.extensions.tenant){
                _.each(customer.extensions.tenant,function(extension,name){
                    if((extensionNames.indexOf(name) != -1) && extension.value && !hasOppExtension(name)){
                        extensions.tenant[name] = extension
                    }
                });
            }
            return extensions;
        }

        var addProjectionExtensions = function(opp,customer){
            var extensions = buildExtensions(opp,customer);
            if(_.keys(extensions.tenant).length){
                _.each(opp.relationships,function(rel){
                    if(rel.relation.name == "customer"){
                        rel.target.extensions = extensions;
                        return false;
                    }
                })
            }
            return extensions;
        }

        var findCustomer = function(opportunity,callback){
            var customerRel = relHelper.getRelationship(opportunity,"customer");
            if(customerRel.length){
                contactsCollection.find({"displayName":customerRel[0].target.displayName},{limit:1},function(err,contacts){
                    if(err){
                        console.log("Can't find customer: '" + customerRel[0].target.displayName + "' due error ". JSON.stringify(err));
                        callback();
                    }else{
                        if(contacts && contacts.length){
                            callback(null,contacts[0]);
                        }else{
                            console.log("Can't find customer '" + customerRel[0].target.displayName + "'");
                            callback();
                        }
                    }
                })
            }else{
                callback();
            }

        }


        async.forEachSeries(oppList,function(opportunity,callback){
            async.waterfall([
                function(callback){findCustomer(opportunity,callback)}
            ],function(err,contact){
                if(contact){
                    var extensions = addProjectionExtensions(opportunity,contact);
                    if(dryRun == false){
                        oppCollection.update(opportunity,function(err){
                            if(err){
                                console.log("Error: can't update opportunity '" + opportunity.displayName + "' due error: " + JSON.stringify(err));
                            }else{
                                console.log("Success: opportunity '" + opportunity.displayName + "' successfully updated with '" + JSON.stringify(extensions)+ "'");
                                updatedOpportunities += 1;
                            }
                            callback();
                        })
                    }else{
                        console.log("Dry Run Success: opportunity '" + opportunity.displayName + "' successfully updated with '" + JSON.stringify(extensions) + "'");
                        updatedOpportunities += 1;
                        callback();
                    }
                }else{
                    callback();
                }
            })
        },function(){
            callback()
        });

    }



    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            findOpportunities(function(){
                console.log("Found opportunities: " + totalOpportunities);
                addProjection(function(){
                    console.log("Updated Opportunities: " + updatedOpportunities);
                })
            });
        }

    }
}

new Scrub().execute(false);



