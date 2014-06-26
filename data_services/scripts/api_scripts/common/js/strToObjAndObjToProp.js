#!/usr/bin/env node


/*

To test the code  execute following command  , program convert string to object and then list properties of the object

./buildJSONfromdotnotation.js  -f "extensions.master.targetPeriod.value.name:fy15Q1,extensions.master.businessLine.value.name:AV,extensions.master.externalIds.tenant.sellingPeriod.value.name:fy14q4,_id:23474373,extensions.master.businessLine.state.name:CA"

*/

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility to export for any object in Renew, using a CSV input.\
        \n\nUsage: $0')
    .alias('f', 'filter').describe('f', 'Generic filter associated with the opportunities to download. JSON string input required')
    .argv;

var filtersplit = input.filter.split(",");

function toArray( obj) {

  var objArray=[];
  
  for (var prop in obj) {
      var value = obj[prop];

      if( !isFunctionA(value) ) {
          if (typeof(value) === 'object') {
              objArray = objArray.concat(toArray(value));
          }  else objArray.push(prop+"."+value);
     }
  }

  for(  i=0; i<objArray.length;i++){
        console.log("Object->",objArray[i]);
  }
}

function toArrayInner(obje, propName ) {
    var fieldArray = [], cnt=0;
    if (propName) {
        fieldArray.push(propName+"."+obje);
        cnt++;
    }
    for (var prop in obje) {
      console.log("PROP2==>", prop);
          var value = obje[prop];

          if( !isFunctionA(value) ) {
              if (typeof(value) === 'object') {
                  console.log("cnt >", cnt, "Obj Len >", value.length);
                  var results = toArrayInner(value, "");
                  console.log(typeof("Res type >",results, "Res Len >", results.length));
                  for(  i=cnt; i<results.length;i++){
                            fieldArray.push(prop+"."+results[i]);
                  }
             } else fieldArray.push(prop+"."+value);
          }
  }
  return fieldArray;
}

function isFunctionA(functionToCheck) {
     var getType = {};
     return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
}


var outputobject = {};

function addValueToObj(obj, newProp) {
    newProp = newProp.split("=");

    var path = newProp[0].split(".");
        val = newProp.slice(1).toString(); 

    for (var i = 0, tmp=obj ; i < path.length - 1; i++) {
        // this if/else block checks to see if the element already exists,
        // if it does, it just moves along. If it doesn't, then it spawns
        // ane empty one.
        if (tmp[path[i]]) {
            tmp = tmp[path[i]];
        } else {
 
            tmp = tmp[path[i]] = {};

        }
    }
    // write the value to the final spot of the path
    tmp[path[i]] = val;
    console.log("Object Last Elem =>", tmp,  "obj=>", obj);
}
/*
_.each(filtersplit, function(record) {
    console.log("Record=>", record);
    addValueToObj(outputobject, record);
});
*/
outputobject = h.strToObj(outputobject,input.filter);
var objArray=[];
toArray(outputobject);



