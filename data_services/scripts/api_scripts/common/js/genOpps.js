#!/usr/bin/env node

var _ = require("underscore");
var async = require("async");
var h = require('./helper');
var csvHelperInstance = require("../../lib/helpers/CsvHelper");

var input = require('optimist')
    .usage('\nREADME: This is a utility generates opportunities.\
        \n\nUsage: $0')
    .alias('t', 'tenant').describe('t', 'Specify tenant')
    .alias('h', 'host').describe('h', 'Specify host')
    .alias('s', 'port').describe('s', 'Specify port') 
    .alias('u', 'user').describe('u', 'Specify user')
    .alias('p', 'password').describe('p', 'Specify password') 
    .alias('f', 'filter').describe('f', 'String version of JSON filter').default('f', '{}')    
    .alias('c', 'criteria').describe('c', 'String version of opportunity generation criteria').default('c', '{}')
    .demand(['h', 't', 'f'])
    .argv;

/* 
./genOpps.js --tenant bluecoat -h config-t2.ssi-cloud.com -u bill.moor@bluecoat.com -p passwordone -s 443 -f '{"tags": "tag0526"}' 
         -c 'extensions.master.targetPeriod.value.name:fy15q1,extensions.master.businessLine.value.name:AV,extensions.master.commitLevel.value.name:black'

v1: Opp Gen to get kicked off with a filter & criteria (JSON), monitor completion of the background job, scan every X minutes & complete when the job finishes
v2: Scan assets for required fields to be present + v1
v3: scan of generated opps/offers for validity (all opps should have offers, "Not Determined" should not be in the name, expirationDate cannot be 9999 ....)
v4: streaming find of assets + configurable asset column check + v2
*/
/*
var restApi = h.getAPI(input),
    tenantApi = restApi.setTenant(input.tenant),
    assetCollection = h.getCollection(restApi, 'app.assets'),
    actionCollection = h.getCollection(restApi, 'core.actions');
*/

// Prep the filter
/*
var init = function(callback) {
    try {
        if (_.isString(input.filter)) input.filter = JSON.parse(input.filter);

        // Can only generate opportunities for service assets without opps
        input.filter.associatedOpportunity = false;
        input.filter.type = 'app.asset/service';

        if (_.isString(input.criteria)) input.criteria = JSON.parse(input.criteria);
        callback();
    } catch (err) {
        callback(err);
    }
};
*/
var init = function() {
    f = JSON.parse(input.filter);
    f["associatedOpportunity"] = false;
    f["type"] = 'app.asset/service';
    return f;    
/*
    try {
        if (_.isString(input.criteria)) input.criteria = JSON.parse(input.criteria);
    } catch (err) {
        input.criteria = {};
    }
*/
};

var findAssets = function (callback) {
    var cols = [
        'displayName',
        'externalIds.id',
        'startDate',
        'endDate',
        'extensions.master.clientBatchQuarter.value',
        'extensions.master.targetPeriod.value.name', // check if you need additional columns to look for mandatory fields
    ];

    h.findRecords(assetCollection, {
        multiple: true,
        filter: input.filter,
        limit: 200000,
        columns: cols,
    }, callback);
};

function repeatStr(str,count){
  return new Array(count+1).join(str);
}

var startOppGen = function(callback) {

     var obj={};
     var arrayCriteria = input.criteria.split(',');
        _.each(arrayCriteria, function(strCriteria) {
               var propArray = strCriteria.split(':');
               var propPath = propArray[0].split('.');
               var propVal = propArray.slice(1).toString();

               console.log("propPath=>", propPath, "==  ", propVal);

               for (var i = 0, tmp=obj ; i < propPath.length - 1; i++) {
                        if (tmp[propPath[i]]) {
                           tmp = tmp[propPath[i]];
                           console.log("Object TMP =>", tmp,  "obj=>", obj);
                        } else {

                           tmp = tmp[propPath[i]] = {};
                        }
                console.log ("OBJ=>", obj,  "  ==  ", tmp);
                }
                // write the value to the final spot of the path
                tmp[propPath[i]] = propVal;

        });

console.log("Final Object======>", obj);
/*
        var c  = JSON.parse(input.criteria);

        var obj={};
        _.each(c, function(elem, i) {
             var objStr ='';
             console.log("i==>", i, "==", elem, " length =>", i.length);

             var keyPath = i.split('.');
             var lastKeyIndex = keyPath.length-1;
                 console.log("LastKeyIndex ==>" , lastKeyIndex, "  " , keyPath[lastKeyIndex]);
                   obj = assign(obj, i);

                 for (var i = 0; i < lastKeyIndex; ++ i) {
                   key = keyPath[i];
                   obj = assign(obj, key);
                   console.log("OBject==>", obj);
                   
                 }

                 obj[keyPath[lastKeyIndex]] = elem;

             
             var commStr = i.replace(/\./g,',');
             var objArray = i.split('.');
             console.log("Length=>", objArray.length);
                  objArray.forEach(function( a, n) {
                        if (n < objArray.length) {
                            objStr = objStr + '{"' + a + '":';
                        }
                        if (n == objArray.length -1) {
                             console.log("Before Str=>", objStr);
                             objStr = objStr + '"'+  elem + '"' + repeatStr("}", n+1);
                             console.log("Str=>", objStr);
                        } 
                  });

             if(objStr) { 
                 var strObj =  JSON.parse(objStr);
                 console.log("STROBJ=>", strObj);
             };

             var test = checkNested(strObj, commStr);
             console.log ("TEST=>", test);

             _.extend(obj, strObj);

             console.log("Object obj ===>", obj);
             var keyArray =_.values(obj);
             console.log("KeyArray => ", keyArray);

             console.log("Object keys ===>", Object.keys(strObj));
             console.log("Str Object ===>", strObj);
             console.log("ObjectType ===>", _.isObject(strObj));

        });

//         var c = _.extend(c, r);

//        console.log("final input criteria==>" , c);
//        console.log("final R ==>" , r);


        tenantApi.execute('app.assets', null, 'genOppUsingFindAndProcess', c, function(err, res) {
            if (err || !res || !res.success || !res.data || !res.data['bgJob'] || !res.data['bgJob'][0]) 
                return callback("on gen kickoff " + JSON.stringify(err || res));

            var r = res.data['bgJob'][0];
            var tag = '';
            if (r.name && h.contains(r.name, ':')) {
                tag = r.name.split(':')[2];
            };
        });
 });
*/
}
//todo to call startOppGen
startOppGen(function(err) {
    if (err) h.log('error',err);
});
