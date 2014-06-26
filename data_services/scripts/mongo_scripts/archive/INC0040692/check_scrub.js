var API = require('./../lib/helpers/api'),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    DellTenant = api.getTenant('dell');
var csvHelper = new require("./../lib/helpers/CsvHelper")();
var collectionHelper = new require("./../lib/helpers/Collection");
var opportunityCollection = new collectionHelper(DellTenant,"app.opportunities","app.opportunity");
var relationshipHelper = new require("./../lib/helpers/RelationshipHelper")();
var _ = require("underscore");
var async = require("async");
var fs = require("fs");

var Scrub = function(){

    var dryRun = true;

    var nonProcessedOpportunities = [];

    var trim = function(str){
        return str.replace(/^\s+|\s+$/g, '');
    }

    var getRelTargetNames = function(rels){
        var res = [];
        _.each(rels,function(rel){
            if(rel && rel.target && rel.target.displayName){
                res.push(rel.target.displayName);
            }
        });
        return res.join(",");
    }

    var normalizeCsvSalesRep = function(str){
        var salesReps = str.split(",");
        _.each(salesReps,function(salesRep,index){
            salesReps[index] = trim(salesRep);
        });
        return salesReps.join(",");
    }

    var checkOpportunity = function(record,callback){
        opportunityCollection.find({"displayName":record["Name"]},{"limit":1},function(err,records){
            if(err || !records || !records.length){
                console.log("Error: can't get opportunity '" + record['Name'] + "'");
                callback();
            }else{
                var csvRelationShips = normalizeCsvSalesRep(record["New Rep Assignment"]);
                var currentSalesRepRelations = getRelTargetNames(relationshipHelper.getRelationship(records[0],"salesRep"));
                //console.log([csvRelationShips,currentSalesRepRelations]);
                if(csvRelationShips != currentSalesRepRelations){
                    console.log("Non Processed opportunity: '" + record["Name"] + "'");
                    nonProcessedOpportunities.push(record);
                }
                callback();
            }
        })
    }





    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            csvHelper.readAsObj("./csv/opportunities.csv",function(data){
                async.forEachSeries(data,function(record,callback){
                    checkOpportunity(record,callback);
                },function(){
                    nonProcessedOpportunities.unshift(["Amount","Earliest Expiration Date","Name","customer","Old Rep Assignment","New Rep Assignment","Team Lead","Sales Stage","Country","Direct / Indirect","Client Batch Quarter","Client Territory"]);
                    var data = "";
                    _.each(nonProcessedOpportunities,function(row){
                        data += _.values(row).join(",") + "\r\n";
                    });
                    fs.writeFileSync("csv/non-processed.csv",data);
                    console.log("DONE");
                })
            })
        }

    }
}

new Scrub().execute();
