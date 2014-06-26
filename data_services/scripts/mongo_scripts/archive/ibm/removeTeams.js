#!/usr/bin/env node

var async = require("async");
var commander = require("commander");
var RestApiInterface = require('./../lib/helpers/RestApi');
var TeamHelper = require('./../lib/helpers/TeamHelper');
var RelHelper = require('./../lib/helpers/RelationshipHelper')();

commander.version("1.0")
    .option("-h, --host [s]", "Scpecify Host")
    .option("-p, --port [s]", "Scpecify Port")
    .option("-u, --user [s]", "Scpecify User")
    .option("-up, --user-password [s]", "Scpecify User Password")
    .option("-t, --tenant [s]", "Scpecify Tenant");

commander
    .command("dryRun")
    .description("execute")
    .action(function(cmd){
        Scrub(true).run();
    });

commander
    .command("run")
    .description("execute")
    .action(function(cmd){
        Scrub(false).run();
    });


commander.parse(process.argv);


function Scrub(dryRun) {

    var oldTeams = ["IBM Dublin","IBM Dublin Ops","IBM Zurich", "IBM Zurich Ops", "IBM Vienna", "IBM Vienna Ops"];
    var restApi = new RestApiInterface(commander.host,commander.port,commander.user,commander.userPassword);
    restApi.setTenant(commander.tenant);
    var teamCollection = restApi.getCollection("core.teams","core.team");


    function findTeams(callback) {
        teamCollection.find({},{},callback);
    }

    function removeRolesFromTeam(team,callback) {
        team.roles = [];
        if(dryRun == false){
            teamCollection.update(team,callback);
        }else{
            callback();
        }

    }

    function removeTeam(team,callback){
        removeRolesFromTeam(team,function(err) {
            if(err){
                console.log("Error removing roles from team: '" + team.displayName + "': " + JSON.stringify(err));
                callback();
            }else{
                if(dryRun == false){
                    teamCollection.delete(team,function(err) {
                        if(err) {
                            console.log("Error removing team: '" + team.displayName + "': " + JSON.stringify(err));
                        } else {
                            console.log("Success removing team: '" + team.displayName + "'");
                        }
                        callback();
                    });
                } else {
                    console.log("DryRun Success removing team: '" + team.displayName + "'");
                    callback();
                }

            }
        })
    }

    return{
        run : function() {
            findTeams(function(err,teams) {
                if(err) {
                    console.log("Error found teams: " + JSON.stringify(err));
                } else if(teams && teams.length) {
                    async.forEachSeries(teams,function(team,next){
                        //console.log(team.name);
                        if(oldTeams.indexOf(team.displayName) != -1) {
                            removeTeam(team,next)
                        } else {
                            next();
                        }
                    }, function(){
                        console.log("DONE");
                    })
                } else {
                    console.log("Error found teams");
                }
            })
        }
    }


}