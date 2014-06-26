var async = require('async');

exports.clearUsers = function(opt, gcb) {
    var users = opt.users; // need { name, username } for each user
    var company = opt.company;

    var tenant = 'ibm';

    var api = opt.api;


    var userIds = [];
    var userNames = [];

    var userDocs;
    async.waterfall(
        [
            function(cb) {
                return async.map(users, function(user, _cb) {
                    api.findOne({
                        collection: 'core.contacts',
                        filter: {
                            displayName: user.name
                        }
                    }, _cb);
                }, cb);
            },
            function(_userDocs, cb) {
                // add missing users
                var i = -1;
                async.each(_userDocs, function(doc, _cb) {
                    i ++;
                    var user = users[i];

                    if(!doc) {
                        console.log('creating new contact for ' + user.name);
                        var arr = user.name.split(/\s+/);
                        if(arr.length != 2) return _cb('invalid user name ' + user.name);
                        var firstName = arr[0];
                        var lastName = arr[1];

                        doc = {
                            membership: null,
                            type: 'core.contact/person',
                            displayName: user.name,
                            firstName: firstName,
                            lastName: lastName,
                            "emailAddresses" : [
                                {
                                    "address" : user.username,
                                    "emailType" : {
                                        "name" : "primary"
                                    },
                                    "type" : "core.email"
                                }
                            ]

                        };
                        api.save({
                            collection: 'core.contacts',
                            doc: doc
                        }, _cb);
                    } else {
                        _cb();
                    }
                }, cb);

            },
            function(cb) {
                // reread users
                async.each(users, function(user, _cb) {
                    api.findOne({
                        collection: 'core.contacts',
                        filter: {
                            displayName: user.name
                        }
                    }, function(err, doc) {
                        if(err) return _cb(err);

                        userIds.push(doc._id);
                        userNames.push(user.username);

                        // clear user doc

                        doc.membership = null;
                        doc.relationships =  [
                            {
                                relation: {
                                    name: 'company',
                                    type: 'core.lookup'
                                },
                                target: {
                                    "type" : "core.contact/organization",
                                    "displayName" : company.name,
                                    "key" : company._id
                                }
                            }
                        ];
                        api.update({
                            collection: 'core.contacts',
                            doc: doc
                        }, _cb);

                    })
                }, cb);
            },
            function(cb) {

                console.log('got userIds for cleanup', JSON.stringify(userIds));
                console.log('got userNames for cleanup', JSON.stringify(userNames));

                api.findOne({
                    collection: 'core.tenants',
                    filter: {
                        name: tenant
                    }
                }, cb);
            },
            function(tenant, cb) {
                var newRels = [];

                tenant.relationships.forEach(function(rel) {
                    if(rel.relation.name != 'membership') {
                        newRels.push(rel);
                        return;
                    }
                    var userId = rel.target.key;
                    if(userIds.indexOf(userId) == -1) {
                        newRels.push(rel);
                    } else {
                        console.log('removing from tenant: ' + rel.target.username);
                    }
                });
                tenant.relationships = newRels;

                api.update({
                    collection: 'core.tenants',
                    doc: tenant
                }, cb);
            },
            function(res, cb) {
                console.log('removing from core.memberships');
                api.remove({
                    collection: 'core.memberships',
                    ignoreRefs: true,
                    filter: {
                        username: { $in: userNames }
                    }
                }, cb);
            },
            function(res, cb) {
                console.log('removed from core.memberships: ' + res.count);

                api.find({
                    collection: 'core.teams'
                }, cb);
            },
            function(teams, cb) {

                async.each(teams, function(team, _cb) {
                    var updated = false;
                    var roles = team.roles || [];
                    roles.forEach(function(role) {
                        var newMembers = [];
                        var members = role.members || [];
                        members.forEach(function(memb) {
                            if(userIds.indexOf(memb.target.key) == -1) {
                                newMembers.push(memb);
                            } else {
                                console.log('removing from team: ' + memb.target.displayName);
                                updated = true;
                            }
                        });
                        role.members = newMembers;
                    });
                    if(updated) {
                        api.update({
                            collection: 'core.teams',
                            doc: team
                        }, _cb);
                    } else {
                        _cb();
                    }
                }, cb);
            }
        ],
        function(err) {
            if(err) console.log('failure: ' + err);
            gcb(err);
        }
    );
}
