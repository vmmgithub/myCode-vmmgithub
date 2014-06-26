#!/usr/bin/env node

var async = require('async');
var fs = require('fs');

var Api = require('../../tools/mgr/api');
var mgrUtil = require('../../tools/mgr/mgrUtil');

var opts = mgrUtil.getOpt(process.argv);

var tenant = 'ibm';

var doit = opts.doit;
var host = opts.host;
var port = opts.port;
var ssl = opts.ssl || '0';
var user = opts.user || 'bruce.lewis@' + tenant + '.com';
var pass = opts.pass || 'passwordone';

if(!host || !port) {
    console.log('no host or port');
    process.exit(1);
}

try {
    fs.mkdirSync('backup');
} catch(e) {};

var tag = new Date().getTime();

var backupFile = 'backup/opportunities.' + host + '.' + tenant + '.' + tag + '.json';

var api = new Api({
    host: host,
    port: port,
    ssl: ssl,
    user: user,
    pass: pass,
    tenant: tenant
});

console.log('BF-486 scrub start: tenant=' + tenant + ', host=' + host + ', port=' + port + ' ssl=' + ssl);
if(!doit) {
    console.log('dry run mode (add doit=1 for actual update)');
}

var total;
var updated = 0;
var lookup;
async.waterfall([
    function(cb) {
        console.log('getting lookup values');
        api.findOne({
            collection: 'app.lookups',
            filter: {
                name: 'none',
                group: 'ResultReason'
            }
        }, cb);
    },
    function(_lookup, cb) {
        if(!_lookup) {
            return cb('failed to fetch lookup');
        }
        lookup = _lookup;
        cb();
    },
    function(cb) {
        console.log('fetching opportunities without resultReason');
        api.find({
            collection: 'app.opportunities',
            filter: {
                'extensions.master.resultReason': { $exists: false }
            }
        }, cb);
    },
    function(opps, cb) {
        total = opps.length;
        if(!doit) {
//            console.log('opportunities to update');
//            opps.forEach(function(opp) {
//                console.log(opp._id);
//            });
            console.log('NOT doing update: need to be updated: ' + opps.length + ' opportunities');
            return cb();
        }

        fs.writeFileSync(backupFile, JSON.stringify(opps, null, 2));

        console.log('DOING UPDATE for ' + total + ' opps, BACKUP AT ' + backupFile);
        api.findAndModify({
            collection: 'app.opportunities',
            filter: {
                'extensions.master.resultReason': { $exists: false }
            },
            modify: {
                extensions: {
                    master: {
                        resultReason: { type: 'lookup', value: { type: 'app.lookup', name: 'none', displayName: lookup.displayName, key: null } }
                    }
                }
            }
        }, cb);
    }
], function(err) {
    if(err) {
        console.log('FAILURE: ' + err);
    }
});