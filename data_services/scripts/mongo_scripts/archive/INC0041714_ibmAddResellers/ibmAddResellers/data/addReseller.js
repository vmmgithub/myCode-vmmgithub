var async = require('async');
var extend = require('deep-extend');

var Api = require('./api');
var mgrUtil = require('./mgrUtil');
var clearUsers = require('./clearUsers');

var opts = mgrUtil.getOpt(process.argv);
var config = opts.config;
var apiHost = opts.apihost;
var apiPort = opts.apiport;
var apiSsl = opts.apissl;
var nosleep = opts.nosleep;

if(!config || !apiHost || !apiPort) {
    console.log('usage: ./RUNME.sh apihost=API_HOST_IP apiport=API_PORT_NUMBER apissl=1 dbhost=MONGO_HOST_IP dbport=MONGO_PORT_NUMBER');
    process.exit(1);
}

var conf = mgrUtil.getConfig(config);

var tenant = 'ibm';

var api = new Api({
    tenant: tenant,
    host: apiHost,
    port: apiPort,
    ssl: apiSsl,
    user: 'bruce.lewis@' + tenant + '.com',
    pass: 'passwordone'
});


var RESELLER_TENANT = conf.tenantName;

var RESELLER_NAME = conf.resellerName;

var RESELLER_ADMIN = 're.sell@' + RESELLER_TENANT + '.com';

var RESELLER_DATA = {
    "tenantName": RESELLER_TENANT,
    "tenant":RESELLER_NAME,
    "teamName": RESELLER_TENANT,
    "template":{"name":"cpo-template"},
    "adminUser":RESELLER_ADMIN,
    "adminPassword":"welcome",
    "adminFirstName":"Re",
    "adminLastName":"Sell",
    "parentTeam":{"name" : "accountHeadTeam" },
    "parentRole":{"name" : "channelPartnerTeams" },
    template: { name : 'cpo-template' }
};

var members = conf.members;



// code starts here

console.log('addReseller started for reseller ' + RESELLER_NAME);

var reseller;
var resApi;
var team;
async.waterfall([
    function(cb) {
        // get the reseller
        api.findOne({
            collection: 'core.contacts',
            filter: {
                name: RESELLER_NAME
            }
        }, cb);
    },
    function(_reseller, cb) {
        // check reseller existense
        if(!_reseller) {
            console.log('creating reseller organization ' + RESELLER_NAME);
            var doc = {
                name: RESELLER_NAME,
                type: 'core.contact/organization'
            };
            api.save({
                collection: 'core.contacts',
                doc: doc
            }, function(err, res) {
                if(err) return cb(err);
                reseller = res.docs[0];

                cb();
            });
        } else {
            reseller = _reseller;
            console.log('got existent reseller organization id ' + reseller._id + ', name ' + RESELLER_NAME);
            cb();
        }
    },
    function(cb) {
        console.log('cleanup members');
        clearUsers.clearUsers({
            api: api,
            users: members,
            company: reseller
        }, cb);
    },
    function(cb) {
        // create channel tenant
        var data = RESELLER_DATA;

        data.organization = reseller;

        console.log('creating channel tenant');
        api.runMethod({
            collection: 'core.contacts',
            method: 'createChannelTenant',
            data: data
        }, cb);
    },
    function(data, cb) {
        if(!data.success) {
            var err = data.messages && data.messages[0] && data.messages[0].message && data.messages[0].message.text || 'unknown error';
            if(err == 'The tenant already exists.') {
                console.log('createChannelTenant: tenant already exists, its OK');
            } else {
                console.log('createChannelTenant: error: ' + JSON.stringify(data, null, 2));
                console.log('trying to continue');
            }
        } else {
            console.log('createChannelTenant: create success');
        }

        cb(null, {});
    },
    function(res, cb) {

        // add channel members
        async.map(members, function(item, _cb) {
            console.log('trying to find contact for channel member name: ' + item.name);
            api.findOne({
                collection: 'core.contacts',
                filter: {
                    displayName: item.name
                }
            }, _cb);
        }, cb);
    },
    function(memberDocs, cb) {
        var i = -1;
        async.eachSeries(memberDocs, function(memberDoc, _cb) {
            i ++;

            if(!memberDoc) {
                console.log('FAILED to create user name ' + members[i].name);
                return;
            }

            console.log('adding channel member id ' + memberDoc._id + ', name ' + memberDoc.displayName + ', role ' + members[i].profile);
            members[i].doc = memberDoc;
            api.runMethod({
                collection: 'core.contacts/' + memberDoc._id,
                method: 'addChannelMember',
                data: members[i]
            }, function(err, res) {
                if(!res.success) {
                    var _err = res.messages && res.messages[0] && res.messages[0].message || 'unknown error';
                    console.log('addChannelMember for ' + memberDoc.displayName + ' failed: ' + _err);
                }
                _cb(err, res);
            });

        }, cb);
    },
    function(cb) {

        resApi = new Api({
            host: apiHost,
            port: apiPort,
            ssl: apiSsl,
            tenant: RESELLER_TENANT,
            user: 're.sell@' + RESELLER_TENANT + '.com',
            pass: 'welcome'
        });

        console.log('fetching team');
        resApi.findOne({
            collection: 'core.teams'
        }, cb);
    },
    function(_team, cb) {
        console.log('got team', _team.name);
        team = _team;

        cb();
    }, function(cb) {

        async.eachSeries(members, function(item, _cb) {
            console.log('running method addMember for ' + item.profile + ', name ' + item.doc.displayName);
            var _api = resApi;

            _api.runMethod({
                collection: 'core.teams/' + team._id,
                method: 'addMember',
                data: {
                    member: item.doc._id,
                    role: item.profile
                }
            }, function(err, data) {
                var _msg = 'ok';
                if(!data.success) {
                    _msg = data.messages && data.messages[0] && data.messages[0].message && data.messages[0].message.text || 'unknown message';
                }
                console.log('addMember result for ' + item.profile + ': ' + _msg);
                _cb();
            });

        }, cb);
    },

    function(cb) {
        console.log('generating policies');
        resApi._doRequest('POST', '/rest/api/' + RESELLER_TENANT + '/core.contacts::generatepolicies?synccall=Y', null, '{}', cb);
    },

    function(res, cb) {
        var data;
        try {
            data = JSON.parse(res);
        } catch(e) {};
        if(!data) {
            console.log('FAILURE: failed to parse response from generatepolicies, hoping all ok');
        }

        if(data.success) {
            console.log('generatepolicies: success');
        } else {
            console.log('FAILURE: generatepolicies got error: ' + JSON.stringify(data));
        }

        console.log('refreshing acl');
        var data = { options: { force: true } };
        api._doRequest('POST', '/rest/api/' + tenant + '/refreshacl', null, data, cb);
    },

    function(res, cb) {
        var data;
        try {
            data = JSON.parse(res);
        } catch(e) {};
        if(!data) {
            console.log('FAILURE: failed to parse response from refreshacl, hoping all ok');
            return cb();
        }

        if(data.success) {
            console.log('refreshacl: success');
        } else {
            console.log('FAILURE: refreshacl got error: ' + JSON.stringify(data));
        }
        cb();
    },
    function(cb) {

        // refreshing ACL for reseller
        console.log('refreshing acl for reseller');
        var data = { options: { force: true } };
        resApi._doRequest('POST', '/rest/api/' + RESELLER_TENANT + '/refreshacl', null, data, cb);
    },

    function(res, cb) {
        var data;
        try {
            data = JSON.parse(res);
        } catch(e) {};
        if(!data) {
            console.log('FAILURE: failed to parse response from reseller refreshacl, hoping all ok');
            return cb();
        }


        if(data.success) {
            console.log('reseller refreshacl: success');
        } else {
            console.log('FAILURE: reseller refreshacl got error: ' + JSON.stringify(data));
        }

        cb();
    },

    function(cb) {
        if(nosleep) return cb();

        var sleep = 60;
        console.log('waiting for refreshacl\'s actual finish: sleep for ' + sleep + ' seconds');
        setTimeout(function() {
            cb();
        }, sleep * 1000);
    },

    function(cb) {
        console.log('checking login capability for created members');

        async.eachSeries(conf.members, function(member, _cb) {
            console.log('checking login capability for member ' + member.username);
            var api = new Api({
                tenant: conf.tenantName,
                host: apiHost,
                port: apiPort,
                ssl: apiSsl,
                user: member.username,
                pass: member.password
            });

            api.find({
                collection: 'core.contacts'
            }, function(err, res) {
                if(err) {
                    console.log('FAILED TO LOGIN AS ' + member.username);
                } else {
                    console.log(member.username + ': login ok');
                }
                _cb();
            });
        }, cb);
    },

    function(cb) {
        //console.log('\n*** PLEASE RESTART ALL NODE SERVERS TO MAKE RESELLER MEMBERS WORK ***\n');
        console.log('*** addReseller done ***');
        cb();
    }

], function(err) {
    if(err) {
        console.log('FAILURE: ' + JSON.stringify(err, null, 2));
        process.exit(1);
    }
});

