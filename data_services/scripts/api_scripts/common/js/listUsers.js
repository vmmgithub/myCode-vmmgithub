#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');

var input = require('optimist')
    .usage('\nREADME: This is a utility to list Renew users.\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('b', 'searchBy').describe('b', 'String version of JSON filter').default('b', '{}')
    .alias('p', 'password').describe('p', 'Specify password') 
   .demand(['h', 't'])
    .argv;

var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant);

var COLS = [
  "_id",
  "membership",
  "displayName",
  //"relationships.team.targets.displayName",
  //"relationships.role.targets.displayName",
  //"relationships.company.targets.displayName",
  "jobTitle",
  "uiProfile",
  //"type",
  //"systemProperties.createdOn",
];

var printHeader = function() {
    var s = '';
    _.each(COLS, function(col) {
        s += '"' + col + '",';
    });
    console.log(s);
}

var printUser = function(user) {
    var s = '';
    _.each(COLS, function(col) {
        s += '"' + h.getObjectValueFromPath(user, col) + '",';
    });
    console.log(s);
};

var listUsers = function (filter, callback) {
    var payload = {
      params: {
        start: 0,
        limit: 500,
        columns: COLS
      }
    };

    if (_.isString(filter)) {
        try { 
            filter = JSON.parse(filter);
        } catch (err) {
            h.log('error', 'Using the filter ' + err);
        }
    }
    payload.filter = filter;

    tenantApi.execute('core.contacts', null, 'getUsersWithTeamInfo', payload, function(err, res) {
        if (err || res.success != true || !res || !res.data || !res.data['core.contact'])
            return callback("on getUsersWithTeamInfo " + JSON.stringify(err || res));

        _.each(res.data['core.contact'], printUser);
        callback();

    });
};

printHeader();
listUsers(input.searchBy, function (err) {
    if (err) h.log('error', JSON.stringify(err));
});