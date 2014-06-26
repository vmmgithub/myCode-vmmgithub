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
            var filter = {"extensions.master.batchQuarter.value":"Q1 9999"};
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


    var changeClientBatchQuarter = function(callback){

        var findOffer = function(opportunity,callback){
            offerCollection.find({"relationships.opportunity.targets.key":opportunity._id},{limit:1},function(err,offers){
                if(offers && offers.length && offers[0]){
                    callback(null,offers[0]);
                }else{
                    console.log("Error: Can't find offer for opportunity '" + opportunity.displayName + "'");
                    callback(err,null);
                }
            });
        }

        var findServiceAsset = function(offer,callback){
            if(offer){
                var assetRels = relHelper.getRelationship(offer,"predecessor");
                assetCollection.find({"_id":assetRels[0].target.key},{limit:1},function(err,assets){
                    if(assets && assets.length && assets[0]){
                        callback(null,assets[0]);
                    }else{
                        console.log("Error: Can't find service asset for offer '" + offer.displayName + "'");
                        callback(err,null);
                    }
                })
            }else{
                callback(null,null);
            }

        }

        var buildClientQuarter = function(assetEndDate){
            var quarters = [[1,2,3],[4,5,6],[7,8,9],[10,11,12]];
            var endDate = new Date(assetEndDate);
            var month = (endDate.getMonth() + 1);
            var year = endDate.getFullYear();
            var quarter = null;
            quarters.forEach(function(quarterMonthes,index){
                if(quarterMonthes.indexOf(month) != -1){
                    quarter = "Q" + (index + 1) + " " + year;
                    return false;
                }
            });
            return quarter;
        }

        async.forEachSeries(oppList,function(opportunity,callback){
            async.waterfall([
                function(callback){findOffer(opportunity,callback)},
                function(offer,callback){findServiceAsset(offer,callback)}
            ],function(err,asset){
                if(asset){
                    opportunity.extensions.master.clientBatchQuarter = {
                        type : "string",
                        value : buildClientQuarter(asset.endDate)
                    };
                    opportunity.extensions.master.batchQuarter.value = buildClientQuarter(asset.endDate)
                    if(dryRun == false){
                        oppCollection.update(opportunity,function(err){
                            if(err){
                                console.log("Error: can't update opportunity '" + opportunity.displayName + "' due error: " + JSON.stringify(err));
                            }else{
                                console.log("Success: opportunity '" + opportunity.displayName + "' successfully updated with '" + opportunity.extensions.master.clientBatchQuarter.value + "'");
                                updatedOpportunities += 1;
                            }
                            callback();
                        })
                    }else{
                        console.log("Dry Run Success: opportunity '" + opportunity.displayName + "' successfully updated with '" + opportunity.extensions.master.clientBatchQuarter.value + "'");
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
                changeClientBatchQuarter(function(){
                    console.log("Updated Opportunities: " + updatedOpportunities);
                })
            });
        }

    }
}

new Scrub().execute(false);



