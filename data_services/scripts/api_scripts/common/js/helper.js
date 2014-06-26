#!/usr/bin/env node

var _ = require('underscore');
var async = require("async");
var moment = require('moment');
var colors = require('colors');
var inflection = require('inflection');
var RestApiInterface = require('../../lib/helpers/RestApi');
var fs = require("fs");
var jsonpath = require('JSONPath').eval;
var mysql = require("mysql");

exports.log = log = function(mode, p) {
    if (!p) {
        mode = ''; 
        p = mode;
    }

    var pre = '[' + (new Date()).toISOString() + '][' + mode + '] ';

    if (mode == 'info')
        console.log(colors.green(pre + p));
    else if (mode == 'error')
        console.log(colors.red(pre + p));
    else if (mode == 'debug')
        console.log(pre + p);
    else if (mode == 'warn')
        console.log(colors.yellow(pre + p));
    else
        console.log(pre + p);
};

exports.print = function(mode, arr) {
        var str = '"';
        arr.forEach(function(a, i) {
                a = (a === '"' || a === '""' || a === "'") ? '' : a;
                str += (i < arr.length-1) ? (a + '","') :  (a + '"');
        });
        log(mode, str);
};

exports.isoDate = isoDate = function (dt) {
    if (!_.isEmpty(dt)) {
        var d = moment(dt);
        return (d.year() + '-' + d.month() + '-' + d.day());
    } 
       
    return '';
};

function checkPrecision(val, base) {
    val = Math.round(Math.abs(val));
    return isNaN(val)? base : val;
}

function unformat(value, decimal) {
    value = value || 0;
    if (typeof value === "number") return value;

    // Default decimal point is "." but could be set to eg. "," in opts:
    decimal = decimal || ".";

     // Build regex to strip out everything except digits, decimal point and minus sign:
    var regex = new RegExp("[^0-9-" + decimal + "]", ["g"]),
        unformatted = parseFloat(
            ("" + value)
            .replace(/\((.*)\)/, "-$1") // replace bracketed values with negatives
            .replace(regex, '')         // strip out any cruft
            .replace(decimal, '.')      // make sure decimal point is standard
        );

    // This will fail silently which may cause trouble, let's wait and see:
    return !isNaN(unformatted) ? unformatted : 0;
};

exports.toFixed = toFixed = function(value, precision) {
    precision = checkPrecision(2, 2);
    var power = Math.pow(10, precision);

    // Multiply up by precision, round accurately, then divide and use native toFixed():
    return parseFloat((Math.round(unformat(value) * power) / power).toFixed(precision));
};

// returns multiple records for the same relationships
exports.getRels = getRels =function(object, relName) {
    var f = _.pluck(_.filter(object.relationships, function(r){ return r.relation.name == relName}), 'target');
    if (f) {
        return f;
    }
    else 
        return null;
};

exports.getRel = getRel =function(object, relName) {
    var f = _.find(object.relationships, function(r){ return r.relation.name == relName});
    if (f) 
        return f.target;
    else 
        return null;
};

exports.getTargetPointer = getTargetPointer = function(object) {
    if (!object) return;

    return {
        key: object._id,
        type: object.type,
        displayName: object.displayName
    };
};

exports.strToObj = strToObj = function (obj, criteria) {
     if (!_.isString(criteria)) return {};
     var arrayCriteria = criteria.split(',');
        _.each(arrayCriteria, function(strCriteria) {

               var propArray = strCriteria.split(':');
               var propPath = propArray[0].split('.');
               var propVal = propArray.slice(1).toString();

               for (var i = 0, tmp=obj ; i < propPath.length - 1; i++) {
                        if (tmp[propPath[i]]) {
                           tmp = tmp[propPath[i]];
                        } else {
                           tmp = tmp[propPath[i]] = {};
                        }
                }
                tmp[propPath[i]] = propVal;
        });
     return obj;
};
exports.getRelKey = getRelKey = function(object, relName) {
    var f = _.find(object.relationships, function(r){ return r.relation.name == relName});
    if (f) 
        return f.target && f.target.key;
    else 
        return null;
};

exports.getRelKeys = getRelKeys =function(object, relName) {
    var f = _.filter(object.relationships, function(r){ return r.relation.name == relName});
    if (!_.isEmpty(f)) 
        return _.pluck(_.pluck(f, 'target'), 'key');
    else 
        return [];
};

exports.contains = contains = function(str, prefix) {
    return str.indexOf(prefix) != -1;
};

exports.endsWith = endsWith = function(str, suffix) {
    return str.indexOf(suffix, str.length - suffix.length) !== -1;
};

exports.startsWith = startsWith =function(str, prefix) {
    return str.indexOf(prefix) == 0;
};

exports.noonOffset = noonOffset = function(dt) {
    var a = moment(dt);
    var r = moment.utc([a.years(), a.months(), a.date(), 12]);

    return r.toISOString();
};

exports.getCSVDate = getCSVDate =function(csvRecord, col) {
    var val = _.isEmpty(csvRecord[col]) ? null: moment(csvRecord[col]);
    if(val) noonOffset(val);
    return val;
};

exports.ISODateString = ISODateString =function (d) {
  function pad(n){return n<10 ? '0'+n : n}
  return d.getUTCFullYear()+'-'
      + pad(d.getUTCMonth()+1)+'-'
      + pad(d.getUTCDate())+'T'
      + pad(d.getUTCHours())+':'
      + pad(d.getUTCMinutes())+':'
      + pad(d.getUTCSeconds())+'Z'
};

exports.compareWithDayOffset = compareWithDayOffset =function(val1, val2) {
    var v1 = moment(isoDate(val1));
    var v2 = moment(isoDate(val2));

    return (v1.diff(v2, 'days') > 2);
};

exports.compareWithAmountOffset = compareWithAmountOffset = function(val1, val2) {
    var v1 = toFixed(val1);
    var v2 = toFixed(val2);

    return ((v1 - v2) * (v1 - v2) > 2);
};

// it will be nice to log original value before update/insert/delete
// Let't find how to combine these two operations
//    accessing and updating data at field level for target
exports.deepSet = deepSet =function(obj, path, r, operation) {
    if(!_.isEmpty(obj) && !_.isEmpty(path)) {
        var tree = path.split("."),
            cursor = obj;
        _.each(tree, function(elem, i) {
            if(i+1 == tree.length) {
                if(_.isArray(cursor[elem])) {
                    if (_.isArray(r)) {
                      cursor[elem] = _.union(cursor[elem], r);
                    } else {
                        if (operation == 'update') cursor[elem].push(r);
                        if (operation == 'delete') cursor[elem] = [];
                    }
                } else {
                      cursor[elem] = r;
                }                             
            } else {
                if(!_.isObject(cursor[elem])) {
                    cursor[elem] = {};
                }
                cursor = cursor[elem];          
            }
        });
    }
};

// API Helpers

exports.getAPI = function(input) {
    process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';
    var u = 'bill.moor@'; // 'data.admin@'
    var p = 'passwordone'; //'Pass@word123';

    var api = new RestApiInterface(input.host, (input.port || 443), (input.user ? input.user : (u + input.tenant + '.com')), input.password || p);    
    
    api.setTenant(input.tenant);

    return api;
};

exports.getCollection = getCollection = function(api, name) {
    var n = getCollectionName(name);
    return api.getCollection(n, inflection.singularize(n));
};

exports.getCollectionName = getCollectionName = function(name) {
    var n = name.split('/')[0];
    return inflection.pluralize(n);
};

// Support for convienience conventions with getting column names
function prepColumnNames(columns) {
    if (!columns || _.isEmpty(columns)) return;

    var renewColumns = [];
    _.each(columns, function(col) {
        if (contains(col, '[')) return renewColumns.push(col.split('[')[0]);
        if (startsWith(col, 'relationships')) return renewColumns.push('relationships.' + col.split('.')[1]);
        renewColumns.push(col);
    });

    return renewColumns;
};

exports.findRecords = findRecords = function (collection, input, callback, datacallback) {
    /*
        // 0. Format of the input object
        {
            filter: 'JSON or string version of the query',
            searchBy: 'searchBy with dot notation',
            value: 'value to go along wit searchBy'
            stream: true | false, provides objects as we receive from the server
            streamComplete: true | false, downloads all objects first and then provides one object at a time
            file: fileName to readfrom when already downloaded with streaming,
            columns: ['col1', 'col2'],
            params: {limit: XX, columns: ['col1', 'col2']},
            limit: 5
        }
    */

    // 1. Build the filter with searchBy or by provided clause
    if (!input.filter) {
        input.filter = {}; 
        if (input.searchBy == 'externalIds.id') 
            input.filter[input.searchBy] = {"$regex": "^" + input.value};
        else 
            input.filter[input.searchBy] = input.value;
    } else {
        try {
            if (_.isString(input.filter)) input.filter = JSON.parse(input.filter);
        } catch (err) {
            callback(collection.name + 'Unable to parse the filter clause ' + input.filter + ' :: ' + err);
        }
    }

    if (!input.params) input.params = {};
    if (input.columns) input.params.columns = prepColumnNames(input.columns);
    if (input.limit) input.params.limit = input.limit;

    var streamOptions = {
        fileMode: input.callIteratorAtEnd,
        logJSON: input.logJSON
    };

    // 2. Use streaming option to pull lots of data or regular search
    if (input.stream && !input.file) {
        collection.findStream(input.filter, input.params, streamOptions, datacallback, function(err, records) {
            if (err || (!input.ignoreEmpty && (!records || records.length == 0))) return callback(err || (collection.name + ' No records found '));
            if (input.ignoreEmpty && (!records || records.length == 0)) return callback(null, []);

            //Streaming should expect multiple records always                
            callback(err, records);
        });        
    } else if (input.file) {
        collection.readStream(input.file, input.params, datacallback, function(err, records) {
            if (err || (!input.ignoreEmpty && (!records || records.length == 0))) return callback(err || (collection.name + ' No records found '));
            if (input.ignoreEmpty && (!records || records.length == 0)) return callback(null, []);

            //Streaming should expect multiple records always                
            callback(err, records);
        });        
    } else {
        collection.find(input.filter, input.params, function(err, records) {
            if (err || (!input.ignoreEmpty && (!records || records.length == 0))) return callback(err || (collection.name + ' No records found '));
            if (input.ignoreEmpty && (!records || records.length == 0)) return callback(null, []);

            if (records.length > 1) {
                if (input.multiple) return callback(null, records);
                else return callback(collection.name + 'Found multiple records - ' + records.length);
            }   
            return callback(null, [records[0]]);
        }); 
    }
};

// Target cached functions
var localCache = {}; 
exports.findCachedRecords = function (collection, input, callback) {
    if (!localCache[collection.name]) localCache[collection.name] = {}; 
    if (localCache[collection.name][input.value]) return callback(null, localCache[collection.name][input.value]);

    findRecords(collection, input, function(err, res) {
        if (err) return callback(err);
        localCache[collection.name][input.value] = res && res[0];
        return callback(null, localCache[collection.name][input.value]);
    });
};

// Lookup cache functions
var cache = {};
var loadLookups = function(api, srcCollection, filter, path, callback) {
    if (!callback) return;
    if (!path) return callback('Nothing to lookup');
    if (cache[path]) return callback(null, cache[path]);

    var coll = getCollection(api, srcCollection);
    coll.find(filter, {limit: 500}, function(err, records) {
        if (err) return callback(err);
        if (!cache[path]) cache[path] = [];

        _.each(records, function(lkp) {
            cache[path].push({
                type: lkp.type,
                key: lkp._id,
                displayName: lkp.displayName,
                name: lkp.name,
                value: lkp.value,
            });
        });
        return callback(null, cache[path]);
    });
};

exports.getLookup = getLookup =function(path, name) {
    var retVal;
    var n = '' + name;
    n = n.replace(/[^a-z0-9\s]/gi, '').toLowerCase();

    var p = '' + path;
    p = p.replace('.value.name', '');
    p = p.replace('.name', '');

    if (cache[p]) {
        _.each(cache[p], function(l) {
            if (l.displayName == name || l.name == name 
                || l.displayName.toLowerCase().replace(/[^a-z0-9\s]/gi, '') == n 
                || l.name.toLowerCase().replace(/[^a-z0-9\s]/gi, '') == n)
                retVal = l;
        });
    }
    return retVal;
};

exports.initLookups = initLookups =function(api, source, callback) {
    var lookupConfigs = getCollection(api, 'core.lookup.configs');
    lookupConfigs.find({model: source}, {}, function(err, records) {
        if (err || !records || records.length == 0) 
            return callback(err || 'No groups found');

        async.eachLimit(records, 1, function(lkpConfig, cb) {
            loadLookups(api, lkpConfig.srcCollection, lkpConfig.filter, lkpConfig.propertyPath, cb);
        }, callback);
    });
};

// Utility functions
exports.uploadDocument = uploadDocument = function (tenantApi, filePath, callback) {
    if (_.isEmpty(filePath)) return callback();

    var readStream = fs.createReadStream(filePath);
    readStream.on("error", function (err) {
        return callback(err, null);
    });

    try {
        tenantApi.attachment(readStream, function (err, resp, body) {
            if (err) return callback(err, null);

            var res = JSON.parse(body);
            callback(null, res[0]);            
        });
    } catch (e) {
        return callback(e, null);
    }
};

exports.getFlowState = getFlowState =function(obj, flowName) {
    return (obj && flowName && obj.flows && obj.flows[flowName] &&  obj.flows[flowName].state && obj.flows[flowName].state.name) 
};

// simpler and faster replacement to jsonpath
function getObjectValue(obj, path) {
    if (!obj) return '';
    if (_.isString(obj)) return obj;

    var elems = _.isArray(path) ? path : path.split('.');
    var curr = obj;

    for(var i=0; i < elems.length; i++) {
        var elem = elems[i];
        if (!curr[elem]) return curr[elem];

        if (_.isArray(curr[elem])) {
            if (_.isEmpty(curr[elem])) return [];

            var ret = [];
            _.each(curr[elem], function(ele) {
                ret.push(getObjectValue(ele, _.rest(elems, i+1)));
            });
            return ret;
        }

        curr = curr[elem];
        if (i == elems.length-1) return curr;
    }
};

exports.getObjectValueFromPath = getObjectValueFromPath = function(obj, path) {
    if (!obj) return;

    //support through jsonpath, when there is a question
    if (contains(path, '[') && contains(path, '?')) {
        var r = jsonpath(obj, path);
        if (r && r.length == 1) return _.first(r);
        return r;
    }

    // Vinod's special handling code
    if (contains(path, '[')) {
        var arrayName = path.split('[')[0]; // Name of the Array
        var arrayRest = path.split('[')[1];
        arrayRest = arrayRest.slice(0, -1); // Building array with list of columns 
        var returnString = '';

        if (_.isEmpty(arrayRest)) return returnString; // Empty array

        var arrayElements = arrayRest.split(',');
        var arrayLength = arrayElements.length; // counting elements from array

        var base = jsonpath(obj, arrayName);

        base = base && base[0];
        if (!base) return '';

       _.each(base, function (r) {
            for (i = 0; i < arrayLength; i++) {
                var l = jsonpath(r, arrayElements[i]);

                if (i > 0)  returnString += '::'

                l = (l && l[0] || '');

                returnString += l ;
            }
           returnString += '|';
        });
        return returnString.slice(0, -1);
    }

    if (startsWith(path, 'relationships') && endsWith(path, 'keyNameType')) {
        var relName = path.split('.')[1];
        var targets = getRels(obj, relName);
        if (targets) {
            var s = '';
            if (endsWith(path, '.keyNameType')) s+= _.first(_.compact(_.pluck(targets, 'type') || []) || []) + '^';
            _.each(targets, function (item) {
                if (endsWith(path, 'key'))
                    s += item.key + '|';
                else 
                    s += (item.key + ':' + item.displayName) + '|';
            });
            return s.slice(0, -1);
        } else
            return '::';
    }

    if (startsWith(path, 'relationships') && !endsWith(path, 'keyNameType')) {
        var elems = path.split('.');
        var target = getRel(obj, elems[1]);

        return getObjectValueFromPath(target, _.reduce(_.rest(elems, 3), function(str, c) { if(str == '') return c; else return str + '.' + c}, ''));
    }

    // handle mongo direct export format
    if (path == '_id' && obj._id && obj._id.str) return obj._id.str;

    return getObjectValue(obj, path);
};

exports.getMasterOpp = getMasterOpp = function(collection, api, opportunity, callback) {
    if (!opportunity || !opportunity._id) return callback('no opportunityId');

    // worker function that checks the flag first, if yes, return immediately
    // if not, look up referers and gets back everyone pointing to it
    // if one of the ones pointing is a master opp, then we get that
    // master opp is the one that has a matching subordinateOpportunity relationship
    var doErfn = function(opp, cb) {
        if (!opp.isSubordinate) return cb(null, opp);

        api.execute('app.opportunities', opp._id, 'referers', {}, function(err, res) {
            if (err || !res || !res.data || !res.data['core.link.references'] || res.data['core.link.references'].length == 0) return cb(err || 'No parent refs found');

            var oIds = _.pluck(_.filter(res.data['core.link.references'], function(r){ return r.type == 'app.opportunity'}), '_id');
            findRecords(collection, {filter: {_id: {$in: oIds}}}, function(err, opps) {
                if(err || !opps || !opps.length) return cb(err || 'No opportunity found from refs');
                
                var masterOpp = _.find(opps, function(o) { return getRelKey(o, 'subordinateOpportunity') == opp._id });
                return cb(null, masterOpp);
            });
        });        
    };

    // If we already have the object with the isSubordinate flag, then its a bit easy
    // If not, it is safer (and slower) to look it up and then proceed
    if (_.find(_.keys(opportunity), function(k){ return k == 'isSubordinate'})) {
        doErfn(opp, callback);
    } else {
        findRecords(collection, {filter: {_id: opportunity._id}}, function(err, opps) {
            var opp = opps && opps[0];
            if (err || !opp)  return callback('no opportunity');

            doErfn(opp, callback);
        });        
    }
};

exports.getPartnerOpp = getPartnerOpp = function(collection, api, opportunity, callback) {
    if (!opportunity || !opportunity._id) return callback('no opportunityId');

    if (opportunity.isSubordinate) return callback(null, opportunity);
    findRecords(collection, {filter: {_id: opportunity._id}}, function(err, opps) {
        var opp = opps && opps[0];
        if (err || !opp) return callback('no valid opportunity');

        var pId = getRelKey(opportunity, 'primaryOpportunity') || getRelKey(opportunity, 'subordinateOpportunity');
        if (!pId) return callback(null, opp);
        findRecords(collection, {filter: {_id: pId}}, function(err, opps) {
            return callback(err, opps && opps[0]);
        });
    });        
};

// SQL Functions
//GLOBALS
var STATS_TABLE = 'JOB_STATUSES',
    RELS_TABLE = 'RELATIONSHIPS',
    STATS_COLS = ['job', 'tableName', 'startDate', 'description', 'numberRecords', 'numberErrors', 'status', 'message', 'endDate', 'updateDate'],
    REFS_COLS = ['sourceTable', 'sourceKey', 'destTable', 'destKey', 'destName', 'relName'];

exports.sqlize = sqlize = function (str) {
    if (!str) return;
    return str.toUpperCase().replace(/\./g,'_').replace(/[^\w\s]/gi, '');
};

exports.sqlizeTable = sqlizeTable = function (str) {
    if (_.isEmpty(str)) return;
    return sqlize(inflection.pluralize(str));
};

exports.getSQLType = getSQLType = function (str) {
    if (!str) return;
    str = str.toLowerCase();

    if (str == '_id') return 'VARCHAR(24)';
    if (startsWith(str, 'relationships')) return 'VARCHAR(24)';
    if (endsWith(str, 'mount')) return 'NUMERIC(20,2)';
    if (startsWith(str, 'number')) return 'INTEGER';
    if (endsWith(str,'date')) return 'TIMESTAMP';
    if (endsWith(str,'createdOn') || endsWith(str, 'odifiedOn')) return 'TIMESTAMP';
    if (str == 'text' || str == 'note' || str == 'message') return 'TEXT';

    return 'VARCHAR(250)';    
};

var Stats = function(job, tableName, description) {
    this.job = job;
    this.tableName = sqlizeTable(tableName);
    this.description = description;
    this.numberRecords = 0;
    this.startDate = new Date();
    this.numberErrors = 0;
    this.endDate = null;
    this.updateDate = null;
    this.message = null;
};

Stats.prototype.incRecords = function() {
    this.numberRecords++;
};

Stats.prototype.incErrors = function() {
    this.numberErrors++;
};

Stats.prototype.markComplete = function(err) {
    this.status = err ? 'ERROR' : 'SUCCESS';
    this.endDate = new Date();
    this.updateDate = new Date();
    this.message = err;
};

exports.Stats = Stats;

var ConnectionHelper = function(input) {
    this.schema = input.tenant;
    this.conn =  mysql.createConnection({
        host     : input.dbhost || 'localhost',
        user     : input.dbuser || 'dataadmin',
        password : input.dbpass || 'passwordone',
        //debug: ['ComQueryPacket', 'RowDataPacket'],
    });
    this.log = input.log;
};

ConnectionHelper.prototype.initConnection = function(callback) {
    var self = this;
    self.conn.connect();
    async.series([
        function(cb) {
            self.conn.query('CREATE DATABASE IF NOT EXISTS ' + self.schema, cb);
        },
        function(cb) {
            self.conn.changeUser({database: self.schema}, cb);
        },
        function(cb) {
            self.createTable(STATS_TABLE, STATS_COLS, false, cb);
        },
        function(cb) {
            self.createTable(RELS_TABLE, REFS_COLS, true, cb);
        },
        function(cb) {
            self.conn.query('set autocommit=0;', cb);
        },
    ], callback);
};

ConnectionHelper.prototype.closeConnection = function(stats, callback) {
    var self = this;
    self.insertRecord(STATS_TABLE, _.keys(stats), _.values(stats), function(err) {
        if (err) return callback(err);

        self.conn.query('commit;', function(err) {
            if (err) return callback(err);
            self.conn.end(callback);
        });
    });
};

ConnectionHelper.prototype.createTable = function(tableName, columns, toDrop, callback) {
    var self = this;
    var dropSQL = 'DROP TABLE IF EXISTS ' + sqlizeTable(tableName) + '; ';
    var createSQL = 'CREATE TABLE ' + sqlizeTable(tableName) + ' (';
    var s = '';

    _.each(columns, function(col) {
        //.keyNameTypes will be stored in a separate table, so we need to exclude those columns
        if (startsWith(col, 'relationships') && endsWith(col, '.keyNameType')) return;
        if (self.log) s += sqlize(col) + ',';

        createSQL+= sqlize(col) + ' ' + getSQLType(col) + ', ';
    });
    
    if (self.log && tableName != RELS_TABLE && tableName != STATS_TABLE) {
        console.log(s);
        return callback();
    }

    createSQL = createSQL.slice(0, -2);
    createSQL+= ');';

    async.series([
        function(cb) {
            if(!toDrop) return cb();
            self.conn.query(dropSQL, cb);
        },
        function(cb) {
            self.conn.query(createSQL, cb);
        },
        /*function(cb) {
            self.index(tableName, cb);
        }*/
        ], 
    callback);
}; 

ConnectionHelper.prototype.persistRecord = function(tableName, obj, callback) {

    var self = this;
    var s = 'INSERT INTO ' + tableName + ' SET ?';
    var p = (tableName == RELS_TABLE) ? 'RELATIONSHIPROWS|' : '';

    if (self.log && tableName != STATS_TABLE) {
        _.each(_.values(obj), function(v) {
            p += ('"' + v + '",');
        });
        console.log(p);
        return callback();
    }

    if (_.isEmpty(obj)) return callback('Empty object');
    self.conn.query(s, obj, callback);
};

ConnectionHelper.prototype.insertRecord = function(tableName, columns, values, callback) {
    tableName = sqlize(inflection.pluralize(tableName));
    
    var self = this,
        obj = {},
        rels = [];

    // Round 1 to construct all attributes except relationships
    _.each(values, function(val,i) {
        if (!self.log && (val == null || val == undefined || val == '')) return;

        //.keyNameTypes will be stored in a separate table, so we need to treat them differently
        if (startsWith(columns[i], 'relationships') && endsWith(columns[i], '.keyNameType')) return;
        obj[sqlize(columns[i])] = val;
    });

    // Round 2 to only get the relationships
    _.each(values, function(val,i) {
        if (val == null || val == undefined || val == '' || !startsWith(columns[i], 'relationships') 
            || !endsWith(columns[i], '.keyNameType') || !contains(val, '^')) return;
            
        // Expecting the format to be type^key1:name1|key2:name2
        if (!contains(val, '^')) return;

        var type = sqlize(getCollectionName(val.split('^')[0]));
        _.each(val.split('^')[1].split('|'), function(v) {
            rels.push({
                sourceTable: tableName,
                sourceKey: obj._ID,
                destTable: type,
                destKey: v.split(':')[0],
                destName: v.split(':')[1],
                relName: columns[i].split('.')[1]
            });
        });
    });

    if (_.isEmpty(obj)) return callback('Empty object');

    var done = _.after(rels.length + 1, callback);
    
    self.persistRecord(tableName, obj, done);
    _.each(rels, function(rel) {
        self.persistRecord(RELS_TABLE, rel, done);
    });
}; 

ConnectionHelper.prototype.index = function(tableName, callback) {
    var self = this;
    if (tableName == RELS_TABLE) {
        var s = 'call dataadmin.create_index_if_not_exists(?, RELATIONSHIPS sourcekey); call dataadmin.create_index_if_not_exists(?, RELATIONSHIPS, destkey);';
        self.conn.query(s, [self.schema, self.schema], callback);
    } else {
        var s = 'call dataadmin.create_index_if_not_exists(?,?,?)'
        self.conn.query(s, [self.schema, tableName, '_ID'], callback);
    }
};

exports.ConnectionHelper = ConnectionHelper;
