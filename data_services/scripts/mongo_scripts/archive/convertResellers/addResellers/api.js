var async = require('async');
var assert = require('assert');
var jsonPath = require('JSONPath').eval;
var extend = require('deep-extend');

var Api = function(opt) {
    var username = opt.username || opt.user;
    var password = opt.password || opt.pass;
    var tenant = this._tenant = opt.tenant;
    var host = opt.host;
    var port = opt.port;
    var secure = opt.secure || opt.ssl;
    if(/^\d+$/.test(secure)) secure = secure - 0; // fixup for 0 as string

    assert.ok(username, 'no username');
    assert.ok(password, 'no password');
    assert.ok(tenant, 'no tenant');

    if(secure) {
        this._http = require('https');
    } else {
        this._http = require('http');
    }

    this._MAX_PARALLEL_REQS = 50;
    this._FIND_HARD_LIMIT = 100;

    this._headers = {
        'accept-language' : 'en-US',
        'Content-Type' : 'application/json',
        tenant: tenant,
        Authorization: 'Basic ' + new Buffer(username + ':' + password).toString('base64')
    }

    this._host = host;
    this._port = port;
}

module.exports = Api;

Api.prototype = {
    runMethod: function(opt, gcb) {
        var that = this;

        var collName = opt.collection;
        var methodName = opt.method;
        var methodData = opt.data;

        var url = '/rest/api/' + this._tenant + '/' + collName + '::' + methodName;
        var payload = methodData;

        this._doRequest('POST', url, this._headers, payload, function(err, res) {
            if(err) return gcb(err);

            var data;
            try {
                data = JSON.parse(res);
            } catch(e) {
                return gcb('runMethod: parse reply failed: ' + e.message);
            }

            gcb(null, data);
        });
    },

    metaRemoveModelById: function(opt, gcb) {
        var that = this;

        var modelId = opt.id;

        assert.ok(modelId, 'metaRemoveModelById: !modelId');

        this._doRequest('DELETE', '/rest/api/' + that._tenant + '/metadata/models/' + modelId + '?ignoreRefs=true', this._headers, '', function(err, res) {
            if(err) return gcb(err);
            var data;
            try {
                data = JSON.parse(res);
            } catch(e) {
                return gcb('failed to parse response: ' + e.message);
            };
            if(!data.success) {
                var _err = data.messages && data.messages[0] && data.messages[0].message || 'unknown error';
                return gcb(_err);
            }
            gcb(null, {});
        });
    },

    metaGetModels: function(opt, gcb) {
        var that = this;

        var filter = opt.filter || {};

        var url = '/rest/api/' + that._tenant + '/metadata/models';
        return that._doRequest('GET', url, that._headers, '', function(err, res) {
            var data;
            try {
                data = JSON.parse(res);
            } catch(e) {
                return gcb('failed to parse response: ' + e.message);
            };
            if(!data.success) {
                return gcb(data.messages[0].message);
            }

            // dirty hack
            var gotCollName = Object.keys(data.data)[0];

            var collData = data.data[gotCollName];

            collData = that._filter(collData, filter);

            gcb(null, collData);
        });
    },

    metaUpdateModel: function(opt, gcb) {
        var that = this;

        var model = opt.model;

        assert.ok(model, 'metaUpdateModel: !model');

        model = extend({}, model);

        var modelName = model.name;

        assert.ok(modelName, 'metaUpdateModel: invalid model');

        delete model._id;
        //delete model.type;

        return async.waterfall(
            [
                function(cb) {
                    // we cant use regular find here, since regular find fetches all old versions of model
                    that.find({
                        collection: 'core.metadata.models'
                    }, cb);
                },
                function(res, cb) {
                    var models = res;

                    var filt = models.filter(function(v) { return v.name == modelName });

                    async.each(filt, function(item, _cb) {
                        console.log('removing old model id ' + item._id);

                        that.metaRemoveModelById({
                            id: item._id
                        }, _cb);
                    }, cb);
                },
                function(cb) {
                    var url = '/rest/api/' + that._tenant + '/core.metadata.models';
                    that.save({
                        collection: 'core.metadata.models',
                        doc: model
                    }, cb);
                },
                function(res, cb) {
                    that.refreshMeta({}, cb);
                },
                function(res, cb) {
                    console.log('api.metaUpdateModel: WARN: please restart node servers');
                }
            ],
            function(err) {
                if(err) return gcb('metaUpdateModel: ' + err);
                return gcb(null, {});
            }
        );
    },

    exportMeta: function(opt, gcb) {
        var that = this;

        var collName = opt.collection;

        async.waterfall(
            [
                function(cb) {
                    //POST /rest/api/ibm/metadata/app.lookups::export?file=true&expand=false&extensions=false

                    that._doRequest('POST', '/rest/api/' + that._tenant + '/metadata/' + collName + '::export', that._headers, '', cb);
                },
                function(res, cb) {
                    var data = JSON.parse(res);

                    var gotCollName = Object.keys(data.data)[0];
                    var collData = data.data[gotCollName];

                    cb(null, collData);
                }
            ],
            function(err, res) {
                if(err) return gcb(err);
                gcb(null, res);
            }
        )
    },
    save: function(opt, gcb) {
        var that = this;

        var collName = opt.collection;
        var doc = opt.doc;
        var docsPerReq = opt.docsPerReq;
        var isMeta = opt.isMeta;

        if(!docsPerReq || !Array.isArray(doc)) {
            return this._save(opt, gcb);
        } else {
            var hasWork = true;
            async.whilst(function() {
                return hasWork;
            }, function(cb) {
                var docs = doc.splice(0, docsPerReq);
                if(docs.length == 0) {
                    hasWork = false;
                    return cb();
                }

                that._save({
                    collection: collName,
                    doc: docs,
                    isMeta: isMeta
                }, cb);
            }, function() {
                gcb(null, {});
            });
        }
    },
    _save: function(opt, gcb) {
        var that = this;

        var collName = opt.collection;
        var object = opt.doc;
        var isMeta = opt.isMeta;

        if(isMeta) {
            if(object._id) {
                console.warn('api.save: removing _id');
                delete object._id;
            }
        }

        assert.ok(collName != '', 'no collection');
        //assert.ok(typeof object == 'object' && object !== null, 'no doc');

        async.waterfall(
            [
                function(cb) {
                    var payload = object;
                    var url = '/rest/api/' + that._tenant + '/' + collName;
                    if(isMeta) {
                        url = '/rest/api/' + that._tenant + '/metadata';
                    }
                    that._doRequest('POST', url, that._headers, payload, cb);
                },
                function(res, cb) {
                    var data;
                    try {
                        data = JSON.parse(res);
                    } catch(e) {
                        console.log('failed to parse: ' + res);
                        return cb('failed to parse response: ' + e.message);
                    };
                    if(!data.success) {
                        var _err = data.messages && data.messages[0] && data.messages[0].message || 'unknown error';
                        return cb('collection ' + collName + ' id ' + object._id + ': ' + _err);
                    }

                    var gotCollName = Object.keys(data.data)[0];
                    var collData = data.data[gotCollName];

                    cb(null, {
                        docs: collData
                    });
                }
            ],
            function(err, res) {
                if(err) return gcb('save: ' + err);

                return gcb(null, res);
            }
        );
    },

    removeById: function(opt, gcb) {
        var that = this;

        var collName = opt.collection;
        var id = opt.id;
        var ignoreRefs = opt.ignoreRefs;

        var url = '/rest/api/' + that._tenant + '/' + collName + '/' + id;
        if(ignoreRefs) {
            url += '?ignoreRefs=true';
        }

        this._doRequest('DELETE', url, this._headers, '', function(err, res) {
            if(err) return gcb(err);
            var data;
            try {
                data = JSON.parse(res);
            } catch(e) {
                return gcb('failed to parse response: ' + e.message);
            };
            if(!data.success) {
                var _err = data.messages && data.messages[0] && data.messages[0].message || 'unknown error';
                return gcb(_err);
            }
            gcb(null, {});
        });
    },

    remove: function(opt, gcb) {
        var that = this;
        var collName = opt.collection;
        var filter = opt.filter || {};
        var noError = opt.noError;
        var ignoreRefs = opt.ignoreRefs;

        var toRemove;
        async.waterfall([
            function(cb) {
                that.find({
                    collection: collName,
                    filter: filter
                }, cb);
            },
            function(data, cb) {
                toRemove = data.length;
                //console.log('remove: found docs: ' + data.length);
                async.eachLimit(data, that._MAX_PARALLEL_REQS, function(v, _cb) {
                    that.removeById({
                        collection: collName,
                        id: v._id,
                        ignoreRefs: ignoreRefs
                    }, function(err) {
                        if(err) {
                            if(noError) {
                                console.log('api.remove: skipping error: failed to remove id ' + v._id + ': ' + err);
                                return _cb(null);
                            }
                            return _cb(err);
                        }

                        return _cb(null);
                    });
                }, cb);
            }
        ], function(err) {
            if(err) return gcb('remove:' + err);

            gcb(null, {
                count: toRemove
            });
        });
    },

    findOne: function(opt, gcb) {
        this.find(opt, function(err, collData) {
            if(err) return gcb(err);
            gcb(null, collData[0]);
        });
    },

    find: function(opt, gcb) {
        var collName = opt.collection;
        var filter = opt.filter || {};
        var limit = opt.limit;
        var offset = opt.offset || 0;
        var isMeta = opt.isMeta;

        assert.ok(collName != '', 'no collection');

        var that = this;

        async.waterfall([
            function(cb) {
                var payload = JSON.stringify({
                    filter: filter,
                    params: {
                        limit: limit,
                        start: offset
                    }
                });
                var url = '/rest/api/' + that._tenant + '/' + collName + '::find';
                var method = 'POST';
                if(isMeta) {
                    url = '/rest/api/' + that._tenant + '/metadata/' + collName;
                    method = 'GET';
                }
                that._doRequest(method, url, that._headers, payload, cb);
            },
            function(res, cb) {
                var data;
                try {
                    data = JSON.parse(res);
                } catch(e) {
                    return cb('failed to parse response: ' + e.message);
                };
                if(!data.success) {
                    return cb(data.messages[0].message);
                }

                // dirty hack
                var gotCollName = Object.keys(data.data)[0];

                var collData = data.data[gotCollName];
                if(!limit && collData.length == that._FIND_HARD_LIMIT) {
                    // we encountered server find limit, fetch next batch
                    that.find({
                        collection: collName,
                        offset: offset + that._FIND_HARD_LIMIT
                    }, function(err, moreCollData) {
                        if(err) return cb(err);
                        collData = collData.concat(moreCollData);
                        cb(null, collData);
                    });
                } else {
                    cb(null, collData);
                }
            }
        ],
        function(err, collData) {
            if(err) return gcb('find: ' + err);

            return gcb(null, collData);
        });
    },


    update: function(opt, gcb) {
        var that = this;

        var collName = opt.collection;
        var object = opt.doc;

        if(Array.isArray(object)) {
            return async.eachSeries(object, function(item, cb) {
                that.update({
                    collection: collName,
                    doc: item
                }, cb);
            }, gcb);
        }

        var objectId = object._id;
	    delete object._id;

        assert.ok(collName != '', 'no collection');
        assert.ok(typeof object == 'object' && object !== null, 'no doc');

        async.waterfall(
            [
                function(cb) {
                    var payload = object;
                    that._doRequest('POST', '/rest/api/' + that._tenant + '/' + collName + '/' + objectId, that._headers, payload, cb);
                },
                function(res, cb) {
                    var data;
                    try {
                        data = JSON.parse(res);
                    } catch(e) {
                        console.log('update: failed to parse response\n', res);
                        return cb('failed to parse response: ' + e.message);
                    };
                    if(!data.success) {
                        var _err = 'unknown reason';
                        try {
                            _err = data.messages[0].message.text;
                        } catch(e) {};
                        return cb('update failed: ' + _err);
                    }

                    cb(null);
                }
            ],
            function(err) {
                if(err) return gcb('update: ' + err);

                return gcb(null, {});
            }
        );
    },

    refreshMeta: function(opt, cb) {
        this._doRequest('GET', '/rest/api/' + this._tenant + '/metadata::refresh', this._headers, '', function(err, res) {
            if(err) return cb(err);

            var data;
            try {
                data = JSON.parse(res);
            } catch(e) {
                return cb('refreshMeta: failed to parse response: ' + e.message);
            };
            if(!data.success) {
                return cb('refreshMeta: backend returned failure');
            }

            cb(null, {});
        });
    },

    _doRequest: function(method, uri, headers, data, callback)  {
        noTimeout = true;

        if(!headers) headers = this._headers;

        if(data === undefined || data === null) return callback('data is null');

        if(typeof data == 'object') {
            data = JSON.stringify(data);
        }

        headers['Content-Length'] = data.length;

        var options = {
            host : this._host,
            port : this._port,
            path : uri,
            method : method,
            headers : headers
        };
        var responseData = '';

        var request = this._http.request(options, function(response) {
            response.on('data', function (chunk) {
                responseData += chunk;
            });
            response.on('end', function(chunk)  {
                callback(null, responseData);
            });
            response.on('error', function(err)  {
                callback(err);
            });
        });

        if(data && (method === 'POST' || method === 'PUT'))  {
            request.write(data);
        }

        request.end();
    },


    _filter: function(arr, filter) {
        var outArr = [];
        arr.forEach(function(item, i) {

            var found = true;
            Object.keys(filter).forEach(function(key) {
                var val = jsonPath(item, key)[0];
                if(val !== filter[key]) {
                    found = false;
                }
            });
            if(found) {
                outArr.push(item);
            }
        });

        return outArr;
    }

}

