#!/usr/bin/env node

var async = require("async");
var commander = require("commander");
var RestApiInterface = require('./../lib/helpers/RestApi');
var TeamHelper = require('./../lib/helpers/TeamHelper');
var _ = require("underscore");
var fs = require("fs");


commander.version("1.0")
    .option("-h, --host [s]", "Scpecify Host")
    .option("-p, --port [s]", "Scpecify Port")
    .option("-u, --user [s]", "Scpecify User")
    .option("-up, --user-password [s]", "Scpecify User Password")
    .option("-t, --tenant [s]", "Scpecify Tenant");


commander
    .command("dryRunUpdateTeams")
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(true).updateTeamsMembers();
    });


commander
    .command('run')
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(false).updateTeamsMembers();
    });

commander
    .command('removeSupport')
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(false).removeSupportRole();
    });



commander.parse(process.argv);

function Scrub(dryRun){

    var restApi = new RestApiInterface(commander.host,commander.port,commander.user,commander.userPassword);
    restApi.setTenant(commander.tenant);

    var analyzeFoundContacts = function(serverContacts,configContacts){
        var lostContacts = [];
        _.each(configContacts,function(displayName){
            if(!_.where(serverContacts,{displayName:displayName}).length){
                lostContacts.push(displayName);
            }
        });
        return lostContacts;
    }

    var getUsers = function(callback){
        var contactColl = restApi.getCollection("core.contacts","core.contact");
        var config =  require("./config/" + commander.tenant);
        contactColl.find({"emailAddresses.address":{"$in": config.contacts}},{},callback);
    }

    var getTeams = function(callback){
        var teamColl = restApi.getCollection("core.teams","core.team");
        teamColl.find({},{},function(err,teams){
            //fs.writeFileSync(commander.tenant + "-teams.json",JSON.stringify(teams,null,4));
            callback(err,teams);
        });
    }

    var updateTeam = function(team){
        var teamColl = restApi.getCollection("core.teams","core.team");
        delete team.systemProperties.revisionId;
        return function(callback){
            teamColl.update(team,function(err){
                if(err){
                    console.log("Team '" +  team.displayName + "' ERROR: " + JSON.stringify(err));
                }
                callback();
            });
        }
    }

    return{

        updateTeamsMembers : function(){
            async.series({
                contacts : getUsers,
                teams : getTeams
            },function(err,result){
                if(err){
                    console.log("ERROR: " + JSON.stringify(err));
                }else if(result && result.contacts && result.teams){
                    var config =  require("./config/" + commander.tenant);
                    _.each(result.contacts,function(contact){
                        _.each(result.teams,function(team){
                            TeamHelper.removeMemberFromTeam(team,contact._id.toString());
                        });
                    });
                    _.each(config.teams,function(teamConfig,teamDisplayName){
                        var teamByProperty = TeamHelper.searchTeamByProperty(result.teams, teamDisplayName, "displayName");
                        if(teamByProperty){
                            _.each(teamConfig,function(users,roleName){
                                _.each(users,function(userDisplayName){
                                    var contactObj = TeamHelper.searchTeamByProperty(result.contacts, userDisplayName, "displayName");
                                    if(contactObj){
                                        TeamHelper.addMemberToTeam(teamByProperty,"displayName",roleName,contactObj);
                                    }
                                })
                            })
                        }
                    });
                    if(dryRun == false){
                        var funcs = [];
                        _.each(result.teams,function(team){
                            funcs.push(new updateTeam(team));
                        });
                        async.series(funcs,function(){
                            console.log("DONE");
                        })
                    }else{
                        //fs.writeFileSync(commander.tenant + "-teams-update.json",JSON.stringify(result.teams,null,4));
                        console.log("Dry Run DONE");
                    }

                }
            })
        },
        removeSupportRole : function(){
            var newTeamNames = ["switzerlandDirectSales","austriaDirectSales","dublinInsideSales"];
            getTeams(function(err,teams){
                if(err){
                    console.log(["Error:" + JSON.stringify(err)]);
                }else{
                    async.forEachSeries(teams,function(team,next){
                        console.log(team.name);
                        if(newTeamNames.indexOf(team.name) != -1){
                            TeamHelper.removeRole(team,"salesSupport");
                            //next();
                            updateTeam(team)(next);
                        }else{
                            next();
                        }

                    },function(){
                        console.log("DONE");
                    })
                }
            })
        }
    }


}


