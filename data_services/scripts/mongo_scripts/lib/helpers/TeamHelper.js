var _ = require("underscore");

module.exports = function(){

   var TeamHelper = function(teamCollection){

       var me = this;

        me.create = function(team,cb){
            teamCollection.create(team,cb);
        }
   }


    TeamHelper.searchTeamByProperty = function(teams,team,property){
        var res = null;
        _.each(teams,function(t){
            if(t[property] == team){
                res = t;
                return false;
            }
        });
        return res;
    }

    TeamHelper.searchTeam = function(teams,team){
        return _.where(teams,{name:team.name});
    }

    TeamHelper.hasTeam = function(teams,team){
        return this.searchTeam(teams,team).length;
    }

    TeamHelper.checkPreSetSubTeams = function(team){
        var teamNames = [];
        _.each(team.roles,function(role){
            _.each(role.members,function(member){
                if(!member.target.name && member.target.type == "core.team"){
                    teamNames.push(member.target.id);
                }
            });
        });
        return teamNames;
    }

    TeamHelper.updatePreSetSubTeam = function(teamContainer,subTeam){
        _.each(teamContainer.roles,function(role){
            _.each(role.members,function(member){
                if(member.target.id == subTeam.name){
                    delete member.target.id;
                    member.target.key = subTeam._id;
                    member.target.name = subTeam.name;
                    member.target.displayName = subTeam.displayName;
                }
                //console.log(member);
            });
        });
    }

    TeamHelper.removeMemberFromTeam = function(team,memberId){
        _.each(team.roles,function(role){
            var relsToDel = [];
            _.each(role.members,function(rel){
                if(rel.relation.name == "memberPerson" && rel.target.key == memberId){
                    relsToDel.push(rel);
                }
            });
            if(relsToDel.length){
                _.each(relsToDel,function(rel){
                    var index = role.members.indexOf(rel);
                    role.members.splice(index,1);
                })
            }
        });
    }

    TeamHelper.addMemberToTeam = function(team,checkProperty,roleProperty,member){
        _.each(team.roles,function(role){
            if(role[checkProperty] == roleProperty){
                if(!role.members){
                    role.members = [];
                }
                role.members.push({
                    relation : {
                        name : "memberPerson"
                    },
                    target : {
                        key : member._id.toString(),
                        displayName : member.displayName,
                        type : "core.contact/person"
                    }
                });
            }
        });
    }

    TeamHelper.removeRole = function(team,roleName){
        var roleIndexToDelete = null;
        _.each(team.roles,function(role,index){
            if(role.name == roleName){
                roleIndexToDelete = index;
                return false;
            }
        });
        if(roleIndexToDelete != null){
            team.roles.splice(roleIndexToDelete,1);
        }
    }

   return TeamHelper;


}();