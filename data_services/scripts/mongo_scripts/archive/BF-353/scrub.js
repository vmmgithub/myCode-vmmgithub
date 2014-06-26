#!/usr/bin/env node

var csvHelperInstance = require("./../lib/helpers/CsvHelper");
var async = require("async");
var commander = require("commander");
var $ = require("jquery");
var RestApiInterface = require('./../lib/helpers/RestApi');
var _ = require("underscore");
var startLine = 0;


commander.version("1.0")
    .option("-h, --host [s]", "Specify Host")
    .option("-p, --port [s]", "Specify Port")
    .option("-u, --user [s]", "Specify User")
    .option("-up, --user-password [s]", "Specify User Password")
    .option("-t, --tenant [s]", "Specify Tenant")


commander
    .command("dryRun")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(true).run();
    });

commander
    .command("run")
    .description("execute dryRun")
    .action(function (cmd) {
        Scrub(false).run(false);
    });


commander.parse(process.argv);


function Scrub(dryRun) {

    var restApi = new RestApiInterface(commander.host, commander.port, commander.user, commander.userPassword);
    restApi.setTenant(commander.tenant);

    var oppCollection = restApi.getCollection("app.opportunities", "app.opportunity");
    var offerCollection = restApi.getCollection("app.offers", "app.offer");
    var assetCollection = restApi.getCollection("app.assets", "app.asset");

    var LOOKUP = {
        RT_BQ: 'baseQuote',
        RT_PQ: 'primaryQuote',
        RT_LQ: 'latestQuote'
    };


    //extensions.tenant.clientRegion
    //extensions.tenant.clientTerritory
    //extensions.tenant.clientTheatre

    var findOpportunities = function (callback) {
        var start = 0;
        var limit = 50;
        var opportunities = [];
        var findPart = function(){
            oppCollection.find({}, {start: start, limit: limit}, function(err,records){
                if(err){
                    callback(err,null);
                }else if(records && records.length){
                    opportunities = opportunities.concat(records);
                    start += limit;
                    if(records.length == limit){
                        findPart();
                    }else{
                        callback(null,opportunities);
                    }
                }else{
                    callback(null,opportunities);
                }
            });
        }
        findPart();
    }

    var getQuoteKey = function (opp) {
        var quoteKey = undefined,
            keys = {},
            QUOTE_RELS = [ LOOKUP.RT_BQ, LOOKUP.RT_PQ, LOOKUP.RT_LQ ],
            checkRel = function (rel) {
                var relName = rel.relation && rel.relation.name;
                if (_.include(QUOTE_RELS, relName)) {
                    keys[relName] = rel.target && rel.target.key;
                }
            },
            result = opp.result && opp.result.name,
            flow = opp.flows.salesStages.state.name;
        opp.relationships.forEach(checkRel);

        if (result === 'win' || flow === 'poReceived') {
            quoteKey = keys[LOOKUP.RT_PQ] || keys[LOOKUP.RT_LQ] || keys[LOOKUP.RT_BQ];
        } else if (result === 'consolidated') {
            quoteKey = keys[LOOKUP.RT_BQ];
        } else {
            quoteKey = keys[LOOKUP.RT_LQ] || keys[LOOKUP.RT_BQ];
        }
        return quoteKey;
    }

    var findOffer = function (opportunity, callback) {
        offerCollection.find({"relationships.quote.targets.key":getQuoteKey(opportunity)},{start:0,limit:1},callback)
    }

    var findAsset = function(key,callback){
        assetCollection.find({"_id":key},{},callback);
    }

    var extensionsNames = ["clientRegion","clientTerritory","clientTheatre"];

    var copyExtensions = function(opp,asset){
        _.each(asset.extensions.master,function(extension,name){
            if(extensionsNames.indexOf(name) != -1){
                //console.log("Opportunity Extension: '" + name + "'" + opp.extensions.master[name].value.name);
                //console.log("Asset Extension: '" + name + "'" + extension.value.name);
                opp.extensions.master[name] = extension;
            }
        });
    }


    return {


        run: function () {
            findOpportunities(function (err, records) {
                if(err){
                    console.log("Error finding opportunities: " + JSON.stringify(err));
                } else if(records && records.length){
                    console.log("FOUND " + records.length + " opportunities");
                    async.forEachSeries(records,function(opp,next){
                        findOffer(opp,function(err,offers){
                            if(offers && offers.length){
                                var assetRel = _.find(offers[0].relationships,function(rel){
                                    return rel.relation.name == "predecessor";
                                });
                                findAsset(assetRel.target.key,function(err,assets){
                                    if(err){
                                        console.log("Error finding asset for opportunity '" + opp.displayName + "'");
                                        next();
                                    } else if(assets && assets[0]){
                                        copyExtensions(opp,assets[0]);
                                        if(dryRun == false){
                                            oppCollection.update(opp,function(err){
                                                if(err){
                                                    console.log("Error updating opportunity: " + JSON.stringify(err));
                                                } else {
                                                    console.log("Success: opportunity '" + opp.displayName + "' updated");
                                                }
                                                next();
                                            })
                                        } else{
                                            console.log("Dry Run Success: opportunity '" + opp.displayName + "' updated");
                                            next()
                                        }
                                    } else {
                                        console.log("Error can't found assets for opportunity '" + opp.displayName + "'");
                                        next();
                                    }
                                });
                            } else {
                                console.log("Error can't found offers for opportunity '" + opp.displayName + "'");
                                next();
                            }
                        });
                    },function(){
                        console.log("DONE");
                    })
                }
            })
        }

    }


}