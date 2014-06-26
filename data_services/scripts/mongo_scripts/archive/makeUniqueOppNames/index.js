var async = require('async');
var extend = require('deep-extend');
var fs = require('fs');


var log = function(msg) {
    console.log(msg);
    fs.appendFileSync('./output.log', msg);
}

var Api = require('../../tools/mgr/api');
var mgrUtil = require('../../tools/mgr/mgrUtil');

var opts = mgrUtil.getOpt(process.argv);

var tenant = opts.tenant;
var host = opts.host;
var port = opts.port;
var ssl = opts.ssl;
var doit = opts.doit;
var backupDir = opts.backupDir || '/tmp/makeUniqueOppNamesBackup_' + new Date().getTime();
var restoreDir = opts.restoreDir;

if(!host || !port || !tenant) {
    log('usage: node ./index.js tenant=TENANT host=API_HOST port=API_PORT [ ssl=1 ] [ doit=1 ]');
    log('default mode is dry run, real updates only with doit=1 flag');
    process.exit(1);
}

var user = 'bill.moor@' + tenant + '.com';
var pass = 'passwordone';


var api = new Api({
    tenant: tenant,
    host: host,
    port: port,
    ssl: ssl,
    user: user,
    pass: pass
});

var oldOpps = [];
var newOpps = [];
async.waterfall([
    function(cb) {
        if(restoreDir) {
            var bckOpps = JSON.parse(fs.readFileSync(restoreDir + '/opportunities.json'));
            log('restoring ' + bckOpps.length + ' opptys from ' + restoreDir);
            async.each(bckOpps, function(opp, _cb) {
                api.update({
                    collection: 'app.opportunities',
                    doc: opp
                }, function(err, res) {
                    if(err) console.log('restore: update failed for "' + opp.displayName + '": ' + err);
                    _cb();
                });
            }, function(err, res) {
                if(err) return cb(err);
                cb(null, { restored: true });
            });
        } else {
            cb(null, {});
        }
    },
    function(res, cb) {
        if(res.restored) {
            process.exit(0);
        }

        if(!doit) {
            log('DRY RUN MODE, add doit=1 for real change');
        } else {
            log('Backing up modified opps to ' + backupDir);
        }
        cb();
    },
    function(cb) {
        api.find({
            collection: 'app.opportunities'
        }, cb);
    },
    function(opps, cb) {
        console.log('got ' + opps.length + ' opptys to check');
        // find opps with same name
        opps.forEach(function(opp) {
            var oldName = opp.displayName;
            var oldOpp = extend({}, opp);
            while(hasDup(opp, opps)) {
                var name = opp.displayName;
                var m = name.match(/^(.*\-)(\d)$/);
                if(m) {
                    opp.displayName = m[1] + (m[2] - 0 + 1);
                } else {
                    opp.displayName = name + '-1';
                }
            }
            if(opp.displayName != oldName) {
                log('change id ' + opp._id + ' "' + oldName + '" -> "' + opp.displayName + '"');
                newOpps.push(opp);
                oldOpps.push(oldOpp);
            }
        });
        cb();
    },
    function(cb) {
        if(doit) {
            try {
                fs.mkdirSync(backupDir);
            } catch(e) {};
            fs.writeFileSync(backupDir + '/opportunities.json', JSON.stringify(oldOpps));
            if(newOpps.length > 0) log('DOING UPDATE for ' + newOpps.length + ' opptys');
            async.each(newOpps, function(opp, _cb) {
                api.update({
                    collection: 'app.opportunities',
                    doc: opp
                }, function(err, res) {
                    if(err) console.log('update failed for "' + opp.displayName + '": ' + err);
                    _cb();
                });
            }, cb);
        }
    },
    function(cb) {
        log('finished');
        cb();
    }
], function(err) {
    if(err) log('FAILURE: ' + err);
});


function hasDup(opp, opps) {

    for(var i = 0; i < opps.length; i ++) {
        var _opp = opps[i];
        if(opp._id != _opp._id && opp.displayName.toLowerCase() == _opp.displayName.toLowerCase()) return true;
    }

    return false;
}