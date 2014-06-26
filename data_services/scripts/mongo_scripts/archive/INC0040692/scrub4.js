var API = require('./../lib/helpers/api'),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    DellTenant = api.getTenant('dell');
var csvHelper = new require("./../lib/helpers/CsvHelper")();
var collectionHelper = new require("./../lib/helpers/Collection");
var lookupCollection = new collectionHelper(DellTenant,"core.lookups","core.lookup");
var contactCollection = new collectionHelper(DellTenant,"core.contacts","core.contact");
var opportunityCollection = new collectionHelper(DellTenant,"app.opportunities","app.opportunity");
var relationshipHelper = new require("./../lib/helpers/RelationshipHelper")();
var _ = require("underscore");
var async = require("async");

var Scrub = function(){

    var dryRun = true;

    var getListPart = function(list,start){
        var result = [];
        _.each(list,function(item,index){
            if(index >= start){
                result.push(item);
            }
        });
        return result;
    }

    var contactCache = {};


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
        opportunityCollection.find({"displayName":displayName},{limit:1},function(err,records){
            if(err){
                console.log("Error: can't found opportunity '" + displayName + "' due error '" + JSON.stringify(err) + "'");
                callback(err);
            }else{
                callback(null,records && records.length && records[0]);
            }
        })
    }

    var findContact = function(displayName,callback){
        contactCollection.find({"displayName":displayName},{limit:1},function(err,records){
            if(err){
                console.log("Error: can't found contact '" + displayName + "' due error '" + JSON.stringify(err) + "'");
                callback(err);
            }else{
                if(records && records.length && records[0]){
                    contactCache[displayName] = records[0];
                }
                callback(null,contactCache[displayName]);
            }
        })
    }

    var getContact = function(displayName,callback){
        if(contactCache[displayName]){
            callback(null,contactCache[displayName])
        }else{
            findContact(displayName,callback);
        }
    }


    var validateResults = function(results,salesRepList,data){
        var result = true;
        if(!results.Opportunity){
            console.log("Error: Opportunity '" + data["Name"] + "' not found");
            return false;
        }
        _.each(salesRepList,function(displayName){
            if(!results[displayName]){
                result = false;
                console.log("Error: Contact '" + displayName + "' not found");
                return false;
            }
        });
        return result;
    }



    var updateOpportunity = function(results,lookup,salesRepList,callback){
        var opp = results.Opportunity;
        var rels = _.reject(opp.relationships,function(rel){
            return rel.relation && rel.relation.name == "salesRep";
        });
        _.each(salesRepList,function(salesRep){
            rels.push(relationshipHelper.buildRelationship(lookup,results[salesRep]));
        });
        opp.relationships = rels;
        if(dryRun == false){
            opportunityCollection.update(opp,function(err){
                if(err){
                    console.log("Error: can't update opportunity due error: " + JSON.stringify(err));
                }else{
                    console.log("Success: opportunity '" + opp.displayName + "' successfully updated with salesReps: '" + salesRepList.join(",") + "'");
                }
                callback();
            })
        }else{
            console.log("Dru Run Success: opportunity '" + opp.displayName + "' successfully updated with salesReps: '" + salesRepList.join(",") + "'");
            callback();
        }

    }


    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            findLookup(function(err,lookup){
                if(!lookup){
                    console.log("Error: Can't found Lookup: 'salesRep' ");
                    return;
                }
                csvHelper.readAsObj("./csv/part4.csv",function(data){
                    data = getListPart(data,7825);
                    async.forEachSeries(data,function(record,callback){
                        var asyncConfig = {};
                        asyncConfig["Opportunity"] = function (callback) {
                            findOpportunity(record["Name"], callback)
                        };
                        var salesRepList = record["New Rep Assignment"].split(",");
                        _.each(salesRepList, function (salesRepName, index) {
                            salesRepList[index] = salesRepName.replace(/^\s+|\s+$/g, '');
                        });
                        _.each(salesRepList, function (displayName) {
                            asyncConfig[displayName] = function (callback) {
                                getContact(displayName, callback)
                            };
                        });
                        async.series(asyncConfig, function (err, results) {
                            if (validateResults(results, salesRepList, record)) {
                                updateOpportunity(results, lookup, salesRepList, function () {
                                    callback()
                                });
                            } else {
                                callback();
                            }
                        })
                    },function(){
                        console.log("DONE");
                    })
                })

            })
        }

    }
}

new Scrub().execute(false);
