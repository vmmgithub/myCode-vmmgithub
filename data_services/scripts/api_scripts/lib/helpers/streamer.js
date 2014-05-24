var _ = require("underscore");
var async = require("async");
var fs = require("fs");
var request = require('request');
var https = require('https');
var moment = require('moment');
var jsonparser = require('./jsonparser').JSONParser;

var getCookie = function(resp) {
    if (!resp.headers || !resp.headers["set-cookie"] || resp.headers["set-cookie"].length == 0) return null;
    return resp.headers["set-cookie"][0].split(';')[0];
};

var http = require('http'),
    httpAgent = http.Agent;
 
httpAgent.maxSockets = 0;

var login = function(host, tenant, user, pwd, cb) {
    var auth = JSON.stringify({username: user, password : pwd});
    var payload = {
        requestCert: true,
        rejectUnauthorized: false,
        port: 443,
        headers: {
            'Content-Length': Buffer.byteLength(auth),
            'content-type': 'application/json; charset=UTF-8',
        },
        url: 'https://' + host + '/login.json',
        body: auth
    };

    request.post(payload, function (err, resp) {
        if (!err && resp && resp.statusCode == 200) 
            cb(err, getCookie(resp));
        else 
            cb('Invalid HTTP message '+ err || resp.statusCode);
    });
};

var getWrapper = function(host, tenant, cookie, content) {
    var l = Buffer.byteLength(JSON.stringify(content));
    return {
        requestCert: true,
        rejectUnauthorized: false,
        port: 443,
        host: host,
        method: 'POST',
        headers: {
            'Connection': 'keep-alive',
            'content-type': 'application/json; charset=UTF-8',
            'Content-Length': '' + l,
            'Referer': 'https://' + host + '/tests/test.html',
            'tenant': tenant,
            'Cookie': cookie,
        }
    };
};

var Streamer = function(host, user, pass) {
    this.host = host;
    this.user = user;
    this.password = pass;
};

Streamer.prototype = _.extend(Streamer.prototype, {
    findStream: function(tenant, collection, query, streamOptions, dcb, ocb) {
        try {
            var self = this;
            var tenantName = tenant.tenant;
            var tmpFileName = '/tmp/' + tenantName + '.' + collection + '.' + self.host + '.' + moment() + '.tmp';
            var parser = new jsonparser();

            async.waterfall([
                // 1. Login into the server
                function(cb) {
                    login(self.host, tenantName, self.user, self.password, cb);
                },

                // 2. Stream the response as objects, as they come
                function(cookie, cb) {
                    var filestream;
                    if (streamOptions.fileMode || streamOptions.logJSON) filestream = fs.createWriteStream(tmpFileName, {flags: 'w'});

                    var payload = getWrapper(self.host, tenantName, cookie, query); 
                    payload.path = '/rest/api/' + tenantName + '/' + collection + '::find';
                    
                    if (!streamOptions.fileMode) {
                        parser.on("data", function(value) {
                            dcb(value);
                        });
                        parser.on("end", cb);
                        parser.on("error", cb);                        
                    }

                    var request = https.request(payload, function (response) {
                        response.on("data", function (chunk) { 
                            if (streamOptions.fileMode) 
                                filestream.write(chunk.toString());
                            else 
                                parser.write(chunk);
                        });
                        response.on("end", function() {
                            parser.close();
                            return cb();
                        });
                        response.on("error", function(err) {
                            parser.close();
                            return cb(err);
                        });
                    });

                    request.setTimeout(1000 * 60000 * 6000 * 1000 * 1000, function () {
                        return cb("Timeout error");
                    });

                    request.write(JSON.stringify(query));
                    request.end();
                }, 
                
                // 3. Read content back from the file
                function(cb) {
                    if (!streamOptions.fileMode) return cb();

                    var stream = fs.createReadStream(tmpFileName);

                    stream.on('data', function (data) {
                        parser.write(data);
                    });
                    stream.on('error', cb);

                    parser.on("data", function(value) {
                        dcb(value);
                    });
                    parser.on("end", cb);
                    parser.on("error", cb);
                }
               ], ocb);
        } catch (err) {
           ocb("Error with processing :" + err);
        }
    },

    readStream: function(tenant, collection, fileName, dcb, ocb) {
        try {
            var self = this;
            var tenantName = tenant.tenant;

            async.waterfall([
                // 1. Stream the response from an input file
                function(cb) {
                    var stream = fs.createReadStream(fileName);
                    var parser = new jsonparser();

                    stream.on('data', function (data) {
                        parser.write(data);
                    });
                    stream.on('error', cb);

                    parser.on("data", function(value) {
                        dcb(value);
                    });
                    parser.on("end", cb);
                    parser.on("error", cb);
                }, 
               ], ocb);
        } catch (err) {
           ocb("Error with processing :" + err);
        }
    },
});

module.exports = Streamer;
