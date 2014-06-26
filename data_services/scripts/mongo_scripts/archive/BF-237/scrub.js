#!/usr/bin/env node

var csvHelperInstance = require("./../lib/helpers/CsvHelper");
var async = require("async");
var commander = require("commander");
var $ = require("jquery");
var RestApiInterface = require('./../lib/helpers/RestApi');
var RelHelper = require('./../lib/helpers/RelationshipHelper')();
var startLine = 0;


commander.version("1.0")
    .option("-h, --host [s]", "Scpecify Host")
    .option("-p, --port [s]", "Scpecify Port")
    .option("-u, --user [s]", "Scpecify User")
    .option("-up, --user-password [s]", "Scpecify User Password")
    .option("-t, --tenant [s]", "Scpecify Tenant")
    .option("-l, --startline [s]", "Scpecify Start Line");


commander
    .command("run")
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(false).run();
    });

commander
    .command("dryRun")
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(true).run();
    });

commander.parse(process.argv);

function Scrub(dryRun){

    var restApi = new RestApiInterface(commander.host,commander.port,commander.user,commander.userPassword);
    var tenant = restApi.setTenant(commander.tenant);
    if(commander.startline){
        startLine = commander.startline;
    }

    var csvHelper = new csvHelperInstance();
    var CSVDocumentPath = "./Assignment-2013.csv";
    var contactCollection = restApi.getCollection("core.contacts","core.contact");
    var oppCollection = restApi.getCollection("app.opportunities","app.opportunity");
    var lookupCollection = restApi.getCollection("core.lookups","core.lookup");

    var UserRegistry = {
        reg : {},
        getUser : function(displayName,callback){
            var me = this;
            if(me.reg[displayName]){
                callback(null,me.reg[displayName]);
            }else{
                contactCollection.find({"displayName" : $.trim(displayName)},{start:0,limit:1},function(err,records){
                    if(err) {
                        callback(err,null);
                    } else if(records && records.length) {
                        me.reg[displayName] = records[0];
                        callback(null,records[0]);
                    } else {
                        callback(null,null);
                    }
                })
            }
        }
    }

    var OppRegistry = {

        getOpp : function(displayName,callback){
            oppCollection.find({"displayName":displayName},{start:0,limit:1},function(err,records){
                if(err) {
                    callback(err,null);
                } else if(records && records.length) {
                    callback(null,records[0]);
                } else {
                    callback(null,null);
                }
            });
        }
    }

    var LookupRegistry = {
        reg : {},
        getLookup : function(name,callback){
            var me = this;
            if(me.reg[name]){
                callback(null,me.reg[name]);
            }else{
                lookupCollection.find({ 'group':'RelationshipType', 'name':'salesRep'},{start:0,limit:1},function(err,records){
                    if(err) {
                        callback(err,null);
                    } else if(records && records.length) {
                        me.reg[name] = records[0];
                        callback(null,records[0]);
                    } else {
                        callback(null,null);
                    }
                })
            }
        }
    }




    return{
        run : function(){
            var count = 0;
            csvHelper.readAsObj(CSVDocumentPath,function(data){
                async.forEachSeries(
                    data,
                    function(rec,next){
                        if(count == startLine){
                            async.series({
                                lookup : function(callback){LookupRegistry.getLookup("salesRep",callback)},
                                contact : function(callback){UserRegistry.getUser(rec.salesRep,callback)},
                                opp : function(callback){OppRegistry.getOpp(rec.name,callback)}
                            },function(err,result){
                                if(err){
                                    console.log("Error: " + JSON.stringify(err));
                                    next();
                                }else{
                                    if(result.opp && result.contact){
                                        RelHelper.removeRelationships(result.opp,"salesRep");
                                        result.opp.relationships.push(RelHelper.buildRelationship(result.lookup,result.contact));
                                        if(dryRun == false){
                                            oppCollection.update(result.opp,function(err){
                                                if(err){
                                                    console.log("Error updating opportunity '" + result.opp.displayName + "': " + JSON.stringify(err));
                                                }else{
                                                    console.log("Success: opportunity '" + result.opp.displayName + "' successfully updated with '" + result.contact.displayName + "' Sales Rep");
                                                }
                                                next();
                                            })
                                        }else{
                                            console.log("Dry Run Success: opportunity '" + result.opp.displayName + "' successfully updated with '" + result.contact.displayName + "' Sales Rep");
                                            next();
                                        }
                                    } else {
                                        if(!result.opp){
                                            console.log("Error can't found opportunity '" + rec.name + "'")
                                        }
                                        if(!result.contact){
                                            console.log("Error can't found contact '" + rec.salesRep + "'")
                                        }
                                        next();
                                    }


                                }
                            });
                        }else{
                            count++;
                            next();
                        }
                    },
                    function(){
                        console.log("DONE")
                    }
                )
            })
        }
    }




}
