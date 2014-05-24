var api_request = require('api_request'),
    _ = require("underscore"),
    request = require("request"),
    fs = require("fs"),
    Streamer = require("./streamer.js");

var http = require('http'),
    httpAgent = http.Agent;
 
httpAgent.maxSockets = 0;

var req = function(api, cb) {
        var headers = { 'accept-language' : 'en-US',
            'Content-Type' : 'application/json' };
        headers.Authorization = 'Basic ' + new Buffer(api.username + ':' + api.password).toString('base64');

        return (new api_request(api.port == '443' || api.port == '8443' ? 'https' : 'http', api.host, api.port))
            .with_content_type("application/json").add_headers(headers)
            .on('reply', function(r) {
                //r = JSON.parse(r);
                if (!r.success) {
                    cb(r.messages);
                } else {
                    cb(null, r);
                }
            }).on('error', function(err) {
                cb(err);
            });
    },
    get = function(api, url, cb) {
        req(api, cb).get(url);
    },
    put = function(api, url, body, cb) {
        req(api, cb).with_payload(body).post(url);
    },
    del = function(api, url, cb) {
        req(api, cb).del(url);
    },
    url = function(tenant, coll, id, action) {
        return "/rest/api/" + tenant + "/" + coll +
            ((id) ? "/" + id : '') +
            ((action)? "::" + action : '');
    };

module.exports = function() {
    var API = function(host, port, username, password) {
        this.host = host || process.env.avalonhost;
        this.port = port || process.env.avalonport;
        this.username = username || process.env.user;
        this.password = password || process.env.pass;
        this.streamer = new Streamer(this.host, this.username, this.password);
    };

    API.prototype = _.extend(API.prototype, {
        getTenant :  function(tenant) {
            return new Tenant(tenant, this);
        },
        getColl : function(tenant, coll) {
            return this.getTenant(tenant).getColl(coll);
        },
        getRecord :  function(tenant, coll, id) {
            return this.getColl(tenant, coll).getRecord(id);
        },
        find :  function(tenant, coll, filter, options, cb) {
            this.getColl(tenant, coll).find(filter, options, cb);
            return this;
        },
        create :  function(tenant, coll, rec, cb) {
            this.getColl(tenant, coll).create(rec, cb);
            return this;
        },
        update: function(tenant, coll, id, rec, cb) {
            this.getRecord(tenant, coll, id).update(rec, cb);
            return this;
        },
        del: function(tenant, coll, id, cb) {
            this.getRecord(tenant, coll, id).del(cb);
            return this;
        },
        execute : function(tenant, coll, id, action, data, cb) {
            this.getRecord(tenant, coll, id).execute(action, data, cb);
            return this;
        },
        getRecordContent : function(tenant, coll, id, cb) {
            this.execute(tenant, coll, id, null, null, cb);
            return this;
        }
    });

    var Tenant = function(tenant, api) {
        this.tenant = tenant;
        this.api = api;
    };

    Tenant.prototype = _.extend(Tenant.prototype, {
        getCollections: function(opt, cb) {
            return this.getMetaData('collections', function(err, res) {
                if(err) throw err;
                var collections = res.data['core.metadata.collections'];
                cb(null, collections);
            });
        },
        getMetaData: function(opt, cb) {
            if(typeof opt == 'string') {
                opt = {
                    op: opt
                }
            }
            var op = opt.op;

            var url = '/rest/api/' + this.tenant + '/metadata/' + op;
            get(this.api, url, cb);
        },
        getColl : function(coll) {
            return new Coll(coll, this);
        },
        getRecord : function(coll, id) {
            return this.getColl(coll).getRecord(id);
        },
        find : function(coll, filter, options, cb) {
            this.getColl(coll).find(filter, options, cb);
            return this;
        },
        create :  function(coll, rec, cb) {
            this.getColl(coll).create(rec, cb);
            return this;
        },
        update: function(coll, id, rec, cb) {
            this.getRecord(coll, id).update(rec, cb);
            return this;
        },
        del: function(coll, id, cb) {
            this.getRecord(coll, id).del(cb);
            return this;
        },
        execStatic :  function(coll, action, body, cb) {
            this.getColl(coll).executeStatic(action, body, cb);
            return this;
        },
        execute : function(coll, id, action, body, cb) {
            this.getRecord(coll, id).execute(action, body, cb);
            return this;
        },
        attachment : function(stream,callback){
            var me = this;
            var protocol =  (me.api.port == '443' || me.api.port == '8443') ? 'https' : "http";
            var url = protocol + "://" + me.api.host + ":" + me.api.port + "/rest/api/" + me.tenant + "/attachments";
            var r = request.post(url,{
                auth : {
                    "user" : me.api.username,
                    "pass" : me.api.password
                }
            },callback);
            var form = r.form();
            form.append("files[]",stream);

        }
    });

    var Coll = function(coll, tenant) {
        this.coll = coll;
        this.tenant = tenant;
    }

    Coll.prototype = _.extend(Coll.prototype, {
        executeStatic : function(action, data, cb) {
            this.getRecord(null).execute(action, data, cb);
            return this;
        },
        find : function(filter, options, cb) {
            var data = { filter : filter, params : options};
            return this.executeStatic('find', data, cb);
        },
        findStream : function(filter, options, streamOptions, dcb, cb) {
            var data = { filter : filter, params : options};
            data.params.limit = -1;
            data.params.stream = "true";

            return this.tenant.api.streamer.findStream(this.tenant, this.coll, data, streamOptions, dcb, cb);
        },
        readStream : function(fileName, options, dcb, cb) {
            return this.tenant.api.streamer.readStream(this.tenant, this.coll, fileName, dcb, cb);
        },
        getRecord : function(id) {
            return new Record(id, this);
        },
        create :  function(rec, cb) {
            this.getRecord(null).execute(null, rec, cb);
            return this;
        },
        update: function(id, rec, cb) {
            this.getRecord(id).update(rec, cb);
            return this;
        },
        del : function(id, cb) {
           this.getRecord(id).del(cb);
            return this;
        }
    });

    var Record = function(id, coll) {
        this.id = id;
        this.coll = coll;
    }

    Record.prototype = _.extend(Record.prototype, {
        execute : function(action, data, cb) {
            exec(this.coll.tenant.api, this.coll.tenant.tenant, this.coll.coll, this.id, action, data, cb);
            return this;
        },
        update : function(rec, cb) {
            return this.execute(null, rec, cb);
        },
        del : function(cb) {
            del(this.coll.tenant.api, url(this.coll.tenant.tenant, this.coll.coll, this.id), cb);
            return this;
        },
        getContent :  function(cb) {
            return this.execute(null, null, cb);
        }
    });

    function exec(api, tenant, coll, id, action, data, cb) {
        var u = url(tenant, coll, id, action);
        if (data) {
            put(api, u, data, cb);
        } else {
            get(api, u, cb);
        }
    };

    return API;
}();
