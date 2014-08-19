#!/usr/bin/env node

var lazy = require("lazy"),
    fs  = require("fs"),
    _ = require("underscore"),
    h = require("../../api_scripts/common/js/helper");

var input = require('optimist')
    .usage('\nREADME: This is a utility to export for any object in Renew, using a CSV input.\
        \n SingleColumn : _id or displayName \
        \n\nUsage: $0')
    .alias('f', 'file').describe('f', 'File Name')
    .alias('c', 'columns').describe('c', 'comma separated list of columns')
    .alias('s', 'source').describe('s', 'name of the collection')
    .demand(['f', 'c', 's'])
    .argv;

input.columns = input.columns.replace(/\|/g,"'");
input.columns = input.columns.split(',');

function printHeader() {
    var s = ''; 
    _.each(input.columns, function(col) {
        if (!h.endsWith(col, 'keyNameType')) s+= sqlize(col) + ',';
    });
    console.log(s);
};

function getRelationship(doc, relation) {
    if (!doc || !doc.relationships || !doc.relationships[relation] || !doc.relationships[relation].targets) return;

    var type = _.first(_.compact(_.pluck(doc.relationships[relation].targets, 'type') || []) || []);

    _.each(doc.relationships[relation].targets, function(target) {       
        console.log('RELATIONSHIPROWS|"' + h.sqlizeTable(input.source) + '","' + h.getObjectValueFromPath(doc, '_id') + '","' + h.sqlizeTable(type) + '","' + target.key + '","' + target.displayName + '","' + relation + '"'); 
    });
};

function printRelationships(doc) {
   _.each(input.columns, function(col) {
      if (h.endsWith(col, 'keyNameType')) {
        getRelationship(doc, col.split('.')[1]);
      }
   });
};

function printVals(doc) {
    var vals = [];
    var s = '';

    _.each(input.columns, function(col) {
        if (!h.endsWith(col, 'keyNameType')) vals.push(h.getObjectValueFromPath(doc, col));
    });

    _.each(vals, function(val) {
        s+= '"' + val + '",';
    });
    console.log(s);
};

var processLine = function(line) {
    try {
        var doc = JSON.parse(line);
        printVals(doc);
        printRelationships(doc);
    } catch (err) {
        h.log("error", "Parsing line " + err);
    }
};

printHeader();
new lazy(fs.createReadStream(input.file))
    .lines
    .forEach(processLine);

