/**
 *
 * EXAMPLE
 *
 * node ./fixQuotePermissions.js host=127.0.0.1 port=7000 tenant=bazaarvoice teams=team1
 *
 */

var async = require('async');

var Api = require('../../tools/mgr/api');
var util = require('../../tools/mgr/mgrUtil');

var opts = util.getOpt(process.argv);

var host = opts.host;
var port = opts.port;
var tenant = opts.tenant;
var ssl = opts.ssl;
var user = opts.user || 'bruce.lewis@' + tenant + '.com';
var pass = opts.pass || 'passwordone';
var otherTeams = opts.teams;
if(otherTeams) {
    otherTeams = otherTeams.split(/,/);
} else {
    otherTeams = [];
}

if(!host || !port || !tenant) {
    console.log('usage: node ./fixQuotePermissions.js teams=team1,team2 tenant=TENANT host=API_HOST port=API_PORT [ ssl=1 ] [ user=USER ] [ pass=PASS ]');
    process.exit(1);
}

var api = new Api({
    host: host,
    port: port,
    tenant: tenant,
    ssl: ssl,
    user: user,
    pass: pass
});

var teamPerms = [{
    "relationships": [],
    "resource": {
        "name": "app.quotes"
    },
    "actions": [
        {
            "name": "read"
        },
        {
            "name": "create"
        },
        {
            "name": "update"
        },
        {
            "name": "delete"
        }
    ]
},
{
    "relationships": [],
    "resource": {
        "name": "app.tasks"
    },
    "actions": [
        {
            "name": "read"
        },
        {
            "name": "create"
        },
        {
            "name": "update"
        },
        {
            "name": "delete"
        }
    ]
}];

var subteamPerms =  [{
        "type": "core.role.permission",
        "condition": "return {};",
        "relationships": [],
        "resource": {
            "name": "app.quotes"
        },
        "relatedResourceAccess": [],
        "actions": [
            {
                "name": "read"
            }
        ]
    },
    {
        "type": "core.role.permission",
        "condition": "return true;",
        "relationships": [],
        "resource": {
            "name": "app.quotes"
        },
        "relatedResourceAccess": [],
        "actions": [
            {
                "name": "update"
            },
            {
                "name": "create"
            },
            {
                "name": "delete"
            }
        ]
    },
{
        "type": "core.role.permission",
        "condition": "return {};",
        "relationships": [],
        "resource": {
            "name": "app.tasks"
        },
        "relatedResourceAccess": [],
        "actions": [
            {
                "name": "read"
            }
        ]
    },
    {
        "type": "core.role.permission",
        "condition": "return true;",
        "relationships": [],
        "resource": {
            "name": "app.tasks"
        },
        "relatedResourceAccess": [],
        "actions": [
            {
                "name": "update"
            },
            {
                "name": "create"
            },
            {
                "name": "delete"
            }
        ]
    }
];

async.waterfall([
    function(cb) {
        api.findOne({
            collection: 'core.teams',
            filter: {
                name: 'accountHeadTeam'
            }
        }, cb);
    },
    function(ahTeam, cb) {
        if(!ahTeam) return cb('no accountHeadTeam');

        console.log('procesing accountHeadTeam');

        ahTeam.availablePermissions = mergePerms(ahTeam.availablePermissions, teamPerms);

        ahTeam.roles.forEach(function(role) {
            if(role.name == 'subTeams') {
                role.permissions = mergePerms(role.permissions, subteamPerms);
            }
        });

        console.log('saving accountHeadTeam');
        api.update({
            collection: 'core.teams',
            doc: ahTeam
        }, cb);
    },
    function(res, cb) {
        async.each(otherTeams, function(teamName, _cb) {
            console.log('processing team ' + teamName);

            api.findOne({
                collection: 'core.teams',
                filter: {
                    name: teamName
                }
            }, function(err, team) {
                if(err) return _cb(err);
                if(!team) {
                    console.log('no team ' + teamName + ', skipping');
                    return _cb();
                }

                if(!team.roles) {
                    console.log('no roles for team ' + teamName + ', skipping');
                    return _cb();
                }
                team.roles.forEach(function(role) {
                    role.permissions = mergePerms(role.permissions, subteamPerms);
                });

                console.log('saving ' + teamName);
                api.update({
                    collection: 'core.teams',
                    doc: team
                }, _cb);

            });

        }, cb);
    }

], function(err) {
    if(err) console.log('FAILURE: ' + err);
});

function mergePerms(base, merge) {
    base = base || [];

    merge.forEach(function(mergeItem) {
        var idx = -1;
        base.forEach(function(baseItem, i) {
            if(mergeItem.resource.name == baseItem.resource.name) idx = i;
        });
        if(idx == -1) {
            console.log('adding persmissions');
            base.push(mergeItem);
        } else {
            console.log('replacing permissions');
            base[idx] = mergeItem;
        }

    });

    return base;
}
