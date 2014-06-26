#!/usr/bin/env node


/*

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

function toArrayInner(obj, path) {
    for (var prop in obj) {
          var value = obj[prop];
          if( !isFunctionA(value) ) {
              if (typeof(value) === 'object') {
                  toArrayInner(value, path+"."+prop);
             } else {
                      fieldArray.push((path+"."+prop+":"+value).replace(/\./,''));
               }
          }
    }
}

strToObj = function (obj, criteria) {
     if (!_.isString(criteria)) return {};

//  spliting string into property
     var arrayCriteria = criteria.split(',');
 
        _.each(arrayCriteria, function(strCriteria) {
               console.log("Criteria ==>", strCriteria, "  ==  ", JSON.stringify(obj));

               var propArray = strCriteria.split(':');
               var propPath = propArray[0].split('.');
               var propVal = propArray.slice(1).toString();
               
               for (var i = 0, tmp=obj; i < propPath.length - 1; i++) {
                        if (tmp[propPath[i]]) {
                           tmp = tmp[propPath[i]];
                        } else {
                           tmp = tmp[propPath[i]] ;
                        }
                    console.log("Tmp Array >", tmp, "  ", tmp[propPath[i]], " I=>", i);
                }
             tmp[propPath[i]] = propVal;
        });
//     return obj;
};


function isFunctionA(functionToCheck) {
     var getType = {};
     return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
}


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

var outputobject = {}, cpyObj={},
    fieldArray=[]; 
strToObj(outputobject,input.filter);
cpyObject=_.external(cpyObj, outputobject
console.log("Returning Object =>", outputobject);
console.log("Returning cpyObject =>", cpyObj);
//console.log("ConvertArray =>", JSON.stringify(outputobject));
//toArrayInner(outputobject, "");
//console.log("FArray =>", fieldArray);
