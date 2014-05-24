#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
 
var input = require('optimist')
    .usage('\nREADME: This is a utility to update gooddata config for any environment\
       \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('n', 'port').describe('n', 'Specify port')
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('j', 'project').describe('j', 'Project Id')
    .alias('d', 'dashboard').describe('d', 'Dashboard Id')
    .alias('o', 'operation').describe('o', 'Operation to perform [log, update]').default('o', 'log')
    .demand(['h', 't'])
    .argv;

var restApi = h.getAPI(input),
    sourceCollection = h.getCollection(restApi, 'core.tenant.config');

var findSource = function (callback) {
    h.findRecords(sourceCollection, {
        multiple: false,
        searchBy: 'appDomain',
        value: 'gooddataConfig',
        ignoreEmpty: true
    }, callback);
};

var logConfig = function(sourceRecord) {
    var p = _.find(sourceRecord.configs, function(c){ return c.name == 'project'});
    var d = _.find(sourceRecord.configs, function(c){ return c.name == 'dashboard'});

    h.log('debug', "Project: '" + (p && p.data && p.data.value) + "', dashboard: '" + (d && d.data && d.data.value) + "'");
}

var createConfig = function(callback) {
    var config = {
      appDomain: "gooddataConfig",
      description: "GoodData Report Configuration",
      displayName: "Report Configuration",
      name: "reportConfig",
      requiredByClient: false,
      configs: [
        {
          name: "project",
          data: {
            value: ('' + input.project)
          }
        },
        {
          name: "dashboard",
          data: {
            value: ('' + input.dashboard)
          }
        }
      ],
      type: "core.tenant.config"
    };       

    sourceCollection.create(config, callback);
};

//     One level nested relationship is supported
var updateConfig = function (sourceRecord, callback) {
    var p = _.find(sourceRecord.configs, function(c){ return c.name == 'project'});
    var d = _.find(sourceRecord.configs, function(c){ return c.name == 'dashboard'});

    if (input.project) {
        var data = {value: '' + input.project};
        if (p) p.data = data;
        else sourceRecord.configs.push({name: 'project', data: data})
    }

    if (input.dashboard) {
        var data = {value: '' + input.dashboard};
        if (d) d.data = data;
        else sourceRecord.configs.push({name: 'dashboard', data: data})
    }

    sourceCollection.update(sourceRecord, callback);
};

findSource(function(err, recs) {
    if (err) return h.log('error', 'looking up config ' + JSON.stringify(err));

    var done = function(err, res) {
        if (err) {
            h.log('error', 'Unable to update config ' + JSON.stringify(err));
        } else {
            h.log('debug', 'Updated config');
            logConfig((res && res.data && res.data['core.tenant.config'] && res.data['core.tenant.config'][0]) || res);
        }
    };

    var config = recs && recs[0];
    if (!config) {
        h.log('error', 'No config present, creating ...');
        return createConfig(done);
    }

    if (input.operation == 'log') {
        logConfig(config);
    } else if (input.operation == 'update') {
        h.log('info','Existing config');
        logConfig(config);
        updateConfig(config, done);
    } else {
        h.log('error', 'Invalid mode');
    }
});

