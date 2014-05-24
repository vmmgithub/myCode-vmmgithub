//Required modules
var _ = require('underscore'),
    async = require('async'),
    api_request = require('api_request'),
    colors = require('colors');

var getDate = exports.getDate = function(simple) {
    var now = new Date(),
        format = null;
    if(!simple) 
        format = now.getFullYear() + "-" + (now.getMonth()+1) + "-" + now.getDate() + "T" + now.getHours() + ":" + now.getMinutes() + ":" + now.getSeconds();
    else 
        format = now.getFullYear() + "-" + (now.getMonth()+1) + "-" + now.getDate() + "T" + now.getHours() + "-" + now.getMinutes() + "-" + now.getSeconds()
    return format;
};

var Logger = function() {
};

Logger.prototype.info = function(str) {
    console.log(colors.green("[linker][" + getDate() + "] " + str));
};

Logger.prototype.debug = function(str) {
    console.log(colors.white("[linker][" + getDate() + "] " + str));
};

Logger.prototype.error = function(str) {
    console.log(colors.red("[linker][" + getDate() + "] " + str));
};

Logger.prototype.warn = function(str) {
    console.log(colors.yellow("[linker][" + getDate() + "] " + str));
};

//Environment level parameters
var env = {
        tenant: process.env.tenant || 'dell',
        avalon: {
            host: process.env.avalonhost || 'localhost',
            port: process.env.avalonport || '7002',
            auth: process.env.auth || 'basic', 
            mode: process.env.avalonport == '443' ? 'https' : 'http',
            user: process.env.avalonuser || 'data.admin@dell.com',
            pwd: process.env.pass || 'Pass@word123'
        }
    },
    L = new Logger();

//Utility functions
var createRequestHeaders = function(username, password, tenant)  {
   var headers = { 'accept-language' : 'en-US',
                   'Content-Type' : 'application/json' };
   if(tenant)  {
       headers.tenant = tenant;
   }
   if(username && password) {
       headers.Authorization = 'Basic ' + new Buffer(username + ':' + password).toString('base64');
   }
   return headers;
};

//Invokes server REST calls
var getAvalon = function() {
    var avalon = new api_request(env.avalon.mode, env.avalon.host, env.avalon.port);
    avalon.with_content_type('application/json');

    if (env.avalon.auth == 'basic') 
        avalon.with_basic_auth(env.avalon.user, env.avalon.pwd)
     else 
        avalon.add_headers(createRequestHeaders(env.avalon.user, env.avalon.pwd, env.tenant))

    return avalon;
},
postAPI = function(url, body, cb) {
     getAvalon().with_payload(body)
     .post(url).on('reply', function(reply, res) {
try { 
        var r = reply; //JSON.parse(reply);
        if (!r.success)
            L.error('postAPI' + 'Error - ' + r.messages[0].message);
        //else
} catch (err) {
	L.error('postAPI' + ' Invalid RoD response ' + reply);
}
        cb(r);
    });
},
getAPI = function(url, cb) {
   getAvalon().get(url).on('reply', function(reply, res) {
try {
        var r = reply; //JSON.parse(reply);
        if (!r.success)
            L.error('getAPI' + 'Error - ' + r.messages[0].message);
} catch (err) {
	L.error('getAPI' + ' Invalid RoD response ' + reply);
}
            cb(r);
    });    
};

var doIt = function(dlQuery, postBody, ocb, retry) {
    var retryThreshold = process.env.retryThreshold || 21; // retry the load after 20 mins
    getAPI('/rest/api/'+env.tenant+'/app.dataloads?query=' + escape(JSON.stringify(dlQuery)), function(res) {
        var loads = res.data["app.dataload"],
            checkIfDone = function(id, cb) {
                getAPI('/rest/api/'+env.tenant+'/app.dataloads/' + id, function(res) {
                    var x = res.data['app.dataload'][0];
                    var finished = false;
                    var curr = 0;

		    if (x.progress) {
                    	curr = _.reduce(_.pluck(_.values(x.progress), 'processed'), function(memo, num){if (num) return memo + num; else return memo; }, 0);
                    	var total = _.reduce(_.pluck(_.values(x.progress), 'total'), function(memo, num){ if (num) return memo + num; else return memo;}, 0);
                    	L.debug('Progress: ' + curr + ' out of ' + total + ' for ' + id + ' ' + x.displayName);
		    }
               	    if (x.status.name == 'completed') finished = true;
               	    return cb(finished, curr);
                });
            };
        var linkFunc = function(dataload) {
            postAPI('/rest/api/'+env.tenant+'/app.dataloads/'+dataload._id+'::postProcess', postBody, function() {
                L.info(' Post process started for ' + dataload._id + ' and ' + dataload.displayName);
                var lastProcessed = 0, retryCount = 0;
                var finished = false;
                var interval = setInterval( function () {
                        checkIfDone(dataload._id, function(isDone, processed) {
                            finished = isDone;
                            if (finished) {
                                clearInterval(interval);
                                ocb(dataload);
                            } else if (retry) {
                                if (processed !== lastProcessed) {
                                    lastProcessed = processed;
                                    retryCount = 0;
                                } else {
                                    retryCount ++;
                                    L.warn('Getting stuck for ' + dataload._id + ":" + dataload.displayName + " for " + retryCount + " time(s).");
                                    if (retryCount === retryThreshold) {
                                        clearInterval(interval);
                                        L.info("Retry the dataload again: " + dataload._id);
                                        linkFunc(dataload);
                                    }
                                }
                            }
                        })
                    },
                    60000);
            });
        }

        _.map(loads, linkFunc);
    });
};

var type = process.env.type || 'covered asset',
    filter = {displayName: {'$regex': type + '.*', '$options': 'i'}},
    dlQuery = {filter: filter, params: {limit: 5000, sort: {'systemProperties.createdOn': 1}}};

// Resume script, useful when restarting linker after cleaning up the environment
task('resumeCruiseControl', function () {
    var payLoad = {parallelChops: 32, recordsPerChop: 250, vintage: false, runOnce:1};
    if (process.env.retry) {
        payLoad = {retry:true};
        dlQuery.filter = {};
        dlQuery.filter['inputSummary.collectionSummary.collectionName'] = 'app.assets';
    }
    dlQuery.filter['status.name'] = 'pending';
    dlQuery.params = {limit: 1};
    var cb = function (load) {
        if (load) {
            L.info("Done postProcess for " + load._id + " " + load.displayName);
            doIt(dlQuery, payLoad, cb, true);
        }
    }
    var count = 0, maxConc = process.env.maxConcurrent || 2;
    var interval = setInterval ( function () {
        if (count>=maxConc) {
            clearInterval(interval);
            return;
        }
        doIt(dlQuery, payLoad, cb, true);
        count ++;
    }, 40000); // kick off every 20 secs to make sure there is no retry
}, {async: true});

task('restartDLPP', function () {
    dlQuery.filter['status.name'] = {'$ne': 'inProgress'};

    doIt(dlQuery, {resume: true}, complete);    
}, {async: true});

//Attempts reprocessing failed records
//Only required if we loaded in additional data and attempting linking again
task('retryDLPP', function () {
    doIt(dlQuery, {retry: true}, complete);    
}, {async: true});

//Attempts reprocessing failed records
//Only required if we loaded in additional data and attempting linking again
task('reprocessDLPP', function () {
    doIt(dlQuery, {reprocess: true}, complete);    
}, {async: true});

/**
 * automatically fill more loads based on the status of server progress
 * (only start if the previous one is done)
 * also has retry logic enabled
 */
task('cruiseControl', function () {
    //var payLoad = {parallelChops: 32, recordsPerChop: 250, vintage: false, retry:1, runOnce:1};
    var payLoad = {parallelChops: 32, recordsPerChop: 250, vintage: false, };
    if (process.env.retry) {
        payLoad = {retry:true};
        dlQuery.filter = {};
        dlQuery.filter['inputSummary.collectionSummary.collectionName'] = 'app.assets';
    }
    dlQuery.filter['status.name'] = 'pending';
    dlQuery.params = {limit: 1, sort: {'systemProperties.createdOn': 1}};
    var cb = function (load) {
        if (load) {
            L.info("Done postProcess for " + load._id + " " + load.displayName);
            doIt(dlQuery, payLoad, cb, true);
        }
    }
    var count = 0, maxConc = process.env.maxConcurrent || 2;
    var interval = setInterval ( function () {
        if (count>=maxConc) {
            clearInterval(interval);
            return;
        }
        doIt(dlQuery, payLoad, cb, true);
        count ++;
    }, 20000); // kick off every 20 secs to make sure there is no retry
}, {async: true});

task('resetDataloadJobs', function () {
    var type = process.env.type,
    filter = {displayName: {'$regex': type + '.*', '$options': 'i'}, "status.name":"inProgress"};

    var q = {filter: filter, params: {limit: 1000}},
        pendingStatus = {name: 'pending', displayName: 'Pending'};

    L.info('Started reset of data load jobs');
    getAPI('/rest/api/'+env.tenant+'/app.dataloads?query=' + escape(JSON.stringify(q)), function(res) {
        if (! res || !res.data || !res.data['app.dataload']) return complete();

        L.info('Updating status on ' + res.data['app.dataload'].length + ' load jobs');
        var done = _.after(res.data['app.dataload'].length, function() {
            L.info('Completed reset of data load jobs');
            complete();
        });
        _.each(res.data['app.dataload'], function(dataload) {
            dataload.status = pendingStatus;
            postAPI('/rest/api/'+env.tenant+'/app.dataloads/' + dataload._id, dataload, done);
        });
    });
}, {async: true});

task('listPendingRecords', function () {
    var q = {filter: {'status.name': {$nin: ['completed', 'purged']}}, params: {limit: 25000}};
    if (process.env.type) q.filter.displayName = {'$regex': type + '.*', '$options': 'i'};

    getAPI('/rest/api/'+env.tenant+'/app.dataloads?query=' + escape(JSON.stringify(q)), function(res) {
        if (! res || !res.data || !res.data['app.dataload']) {
            //L.info('Nothing pending. Party!!');
            return complete();
        }

        var total = {};
        _.each(res.data['app.dataload'], function(dataload) {
            var dn = (dataload.displayName || 'none');
	    L.info(dataload._id + " " + dataload.displayName);
	});
    });
}, {async: true});

task('countPendingRecords', function () {
    var q = {filter: {'status.name': {$nin: ['completed', 'purged']}}, params: {limit: 25000}};
    if (process.env.type) q.filter.displayName = {'$regex': type + '.*', '$options': 'i'};

    getAPI('/rest/api/'+env.tenant+'/app.dataloads?query=' + escape(JSON.stringify(q)), function(res) {
        if (! res || !res.data || !res.data['app.dataload']) {
            //L.info('Nothing pending. Party!!');
            return complete();
        }

        var total = {};
        _.each(res.data['app.dataload'], function(dataload) {
            var s = (dataload.status && dataload.status.name) || 'none';
            var dn = (dataload.displayName || 'none');
	    var type;

	    if (dn.match(/person/i)) type = 'person';
	    if (dn.match(/org/i)) type = 'org';
	    if (dn.match(/affinity/i)) type = 'affinity';
	    if (dn.match(/covered/i) && dn.match(/asset/i)) type = 'covered';
	    if (dn.match(/service/i) && dn.match(/asset/i)) type = 'service';
	    if (dn.match(/offer/i)) type = 'offer';
	    if (dn.match(/opp/i)) type = 'opp';
	    if (dn.match(/booking/i)) type = 'booking';
	    if (dn.match(/quote/i)) type = 'quote';
	    if (dn.match(/addr/i)) type = 'addr';

	    if (!type) {
            	type = (dataload.inputSummary && dataload.inputSummary.collectionSummary && dataload.inputSummary.collectionSummary[0] && dataload.inputSummary.collectionSummary[0].collectionName) || 'none';
	    }

            if (!total[s]) total[s] = {};
            if (!total[s][type]) total[s][type] = {count: 0, type: type, jobs: 0, name: s, unknown: 0};
	    var c = dataload.inputSummary && dataload.inputSummary.collectionSummary && dataload.inputSummary.collectionSummary[0] && dataload.inputSummary.collectionSummary[0].numberRecords;
            if (c) total[s][type].count += c; else total[s][type].unknown ++;
            total[s][type].jobs ++;
        });

        _.each(total, function(s) {
	    _.each(s, function(t) {
                L.info('Linking of ' + t.count + ' and ' + t.unknown + ' unknown [' + t.type + '] records across ' + t.jobs + ' jobs with '+ t.name + ' status');
	    });
        });
    });
}, {async: true});

task('resetIncompleteJobs', function () {
    var q = {filter: {"status.name":{"$!regex":"^"}}, params: {limit: 50000}};
    if (process.env.type) q.filter.displayName = {'$regex': type + '.*', '$options': 'i'};

    L.info('Started reset of data load jobs');
    getAPI('/rest/api/'+env.tenant+'/app.dataloads?query=' + escape(JSON.stringify(q)), function(res) {
        if (! res || !res.data || !res.data['app.dataload']) return complete();

        L.info('Updating status on ' + res.data['app.dataload'].length + ' load jobs');
        var done = _.after(res.data['app.dataload'].length, function() {
            L.info('Completed reset of data load jobs');
            complete();
        });
        _.each(res.data['app.dataload'], function(dataload) {
            postAPI('/rest/api/'+env.tenant+'/app.dataloads/' + dataload._id + '::done', {defer: true}, done);
        });
    });
}, {async: true});

task('countIncompleteJobs', function () {
    var q = {filter: {"status.name":{"$!regex":"^"}}, params: {limit: 50000}};
    if (process.env.type) q.filter.displayName = {'$regex': type + '.*', '$options': 'i'};

    getAPI('/rest/api/'+env.tenant+'/app.dataloads?query=' + escape(JSON.stringify(q)), function(res) {
        if (! res || !res.data || !res.data['app.dataload']) {
            //L.info('Nothing pending. Party!!');
            return complete();
        }

        var total = {};
        _.each(res.data['app.dataload'], function(dataload) {
            var s = (dataload.status && dataload.status.name) || 'none';
            var dn = (dataload.displayName || 'none');
            var type;

            if (dn.match(/person/i)) type = 'person';
            if (dn.match(/org/i)) type = 'org';
            if (dn.match(/affinity/i)) type = 'affinity';
            if (dn.match(/covered/i) && dn.match(/asset/i)) type = 'covered';
            if (dn.match(/service/i) && dn.match(/asset/i)) type = 'service';
	    if (dn.match(/offer/i)) type = 'offer';
	    if (dn.match(/opp/i)) type = 'opp';
	    if (dn.match(/booking/i)) type = 'booking';
	    if (dn.match(/quote/i)) type = 'quote';
	    if (dn.match(/addr/i)) type = 'addr';

            if (!type)
                type = (dataload.inputSummary && dataload.inputSummary.collectionSummary && dataload.inputSummary.collectionSummary[0] && dataload.inputSummary.collectionSummary[0].collectionName) || 'none';

            if (!total[s]) total[s] = {};
            if (!total[s][type]) total[s][type] = {count: 0, type: type, jobs: 0, name: s};
            total[s][type].jobs ++;
        });

        _.each(total, function(s) {
            _.each(s, function(t) {
                L.info('Linking of [' + t.type + '] records across ' + t.jobs + ' jobs with '+ t.name + ' status');
            });
        });
    });
}, {async: true});
