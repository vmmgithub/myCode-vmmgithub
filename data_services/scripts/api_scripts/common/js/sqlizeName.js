#!/usr/bin/env node

var _ = require("underscore");
var h = require('./helper');

var input = require('optimist')
    .usage('Get SQL column name and type.')
    .alias('f', 'field').describe('f', 'Field Name')
    .alias('t', 'table').describe('t', 'Table Name')
    .argv;

if (_.isEmpty(input.field) && _.isEmpty(input.table))
	console.log('Need either field or table name');
else if (!_.isEmpty(input.field)) 
	console.log(h.sqlize(input.field), h.getSQLType(input.field));
else if (!_.isEmpty(input.table)) 
	console.log(h.sqlizeTable(input.table));
