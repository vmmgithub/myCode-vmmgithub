load('./underscore.js');

function sqlize(str) {
    if (!str) return;
    return str.toUpperCase().replace(/\./g,'_').replace(/[^\w\s]/gi, '');
};

function sqlizeTable(str) {
    if (_.isEmpty(str)) return;
    var n = str.split('/')[0];

    switch (n) {
        case 'app.opportunity': return 'APP_OPPORTUNITIES'; break;
        case 'core.contact': return 'CORE_CONTACTS'; break;
        case 'app.asset': return 'APP_ASSETS'; break;
        case 'app.offer': return 'APP_OFFERS'; break;
        case 'app.quote': return 'APP_QUOTES'; break;
        case 'app.booking': return 'APP_BOOKINGS'; break;
        case 'app.product': return 'APP_PRODUCTS'; break;
        case 'app.contact': return 'APP_CONTACTS'; break;
        case 'app.lookup': return 'APP_LOOKUPS'; break;
        case 'app.lineitem': return 'APP_LINEITEMS'; break;
    };
    return sqlize(str);
};

function startsWith(str, prefix) {
    return str.indexOf(prefix) == 0;
};

function printHeader() {
    var s = ''; 
    _.each(cols, function(col) {
        if (col.indexOf('keyNameType') == -1) s+= sqlize(col) + ',';
    });
    print(s);
};

function getValue(obj, path) {
    if (!obj) return;
    if (_.isString(obj)) return obj;

    // array based IF checks
    if (path.indexOf('[?(') != -1) {
        var subPath =  path.substring(0, path.indexOf('['));
        var sRest = path.substring(path.indexOf('['));
        var checkString = sRest.substring(0, sRest.indexOf(']') + 1);
        var subPathRest = sRest.substring(sRest.indexOf(']') + 2);
        var curr = getArrayConditionalValue(getValue(obj, subPath), checkString);
        if (!curr || _.isEmpty(curr)) return;
        var ret = _.map(curr, function(c) { 
                               return getValue(c, subPathRest) });
        return (ret.length == 1 ? ret[0] : ret);
    }

    var elems = path.split('.');
    var curr = obj;

    for(var i=0; i < elems.length; i++) {
        var elem = elems[i];
        
        if (!curr[elem]) return;

        if (_.isArray(curr[elem])) {
            if (_.isEmpty(curr[elem])) return [];

            var ret = [];
            if (i == elems.length-1) return curr[elem];
            _.each(curr[elem], function(ele) {
                ret.push(getValue(ele, _.reduce(_.rest(elems, i+1), function(str, c) { if(str == '') return c; else return str + '.' + c}, '')));
            });
            return ret;
        }

        if (elem == '_id') return curr[elem].valueOf();
        if (_.isDate(curr[elem])) return curr[elem].toISOString();

        curr = curr[elem];
        if (i == elems.length-1) return curr;
    }
};

function getArrayConditionalValue(arr, checkString) {
    var cString = checkString.substring(checkString.indexOf('('), checkString.indexOf(')')+1 );
    cString = cString.replace(/\@/g, 'elem');

    var ret = [];
    _.each(arr, function(elem) {
        if (eval(cString)) ret.push(elem);   
    });

    return ret;
};

// var REFS_COLS = ['sourceTable', 'sourceKey', 'destTable', 'destKey', 'destName', 'relName'];
function getRelationship(doc, relation) {
    if (!doc || !doc.relationships || !doc.relationships[relation] || !doc.relationships[relation].targets) return;

    var type = _.first(_.compact(_.pluck(doc.relationships[relation].targets, 'type') || []) || []);

    _.each(doc.relationships[relation].targets, function(target) {
           var objProp = JSON.stringify(target);
       
        print('RELATIONSHIPROWS|"' + sqlizeTable(coll) + '","' + getValue(doc, '_id') + '","' + sqlizeTable(type) + '","' + target.key + '","' + target.displayName + '","' + relation + '"'); 
    });
};

function toArray( obj) {
  print("Object >"+JSON.stringify(obj));
  for (var prop in obj) {
    var value = obj[prop];

    if( !isFunctionA(value) ) {
      if (typeof value === 'object') {
        var fieldArray = toArrayInner( value);
        for(  i=0; i<fieldArray.length;i++){
print("Chala->"+fieldArray[i]);
}
    } else {
        print(prop+"."+value);
    }
}
  }
}
function toArrayInner( obj) {
        var fieldArray = [];
var counter = 1;
  for (var prop in obj) {
    var value = obj[prop];

        if( !isFunctionA(value) ) {
    if (typeof value === 'object') {
        var results = toArrayInner(value);
        for(  i=0; i<results.length;i++){
        fieldArray.push(prop+"."+results[i]);
}
    } else {
        fieldArray.push(prop+"."+value);
    }
}
  }
        return fieldArray;
}

function isFunctionA(functionToCheck) {
 var getType = {};
 return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
}


function printRelationships(doc) {
   _.each(cols, function(col) {
      if (col.indexOf('keyNameType') != -1) {

        var splitCol = col.split('.');
        var splitCollen = splitCol.length;

        getRelationship(doc, col.split('.')[1]);
      }
   });
};

var printDoc = function(doc) {
    var s = '';
   _.each(cols, function(col) {
      if (col.indexOf('keyNameType') == -1) s += '"' + getValue(doc, col) + '",';
      if (col.indexOf('keyNameType') != -1) printRelationships(doc);
   });
   print(s);
};

var addColumns=addCols.replace(/\|/g,"'");
var cols = addColumns.split(',');
var columns = {};
_.each(cols, function(col) {
    col = col.split('[')[0];
    col = col.replace(/\.keyNameType/g,"");
    columns[col] = 1;
});

printHeader();
db[coll].find({
    'systemProperties.tenant' : tenant,
    'systemProperties.expiredOn' : ISODate('9999-01-01:00:00:00Z'),
}, columns)
.readPref('secondary')
.addOption(DBQuery.Option.noTimeout)
//.limit(5)
.forEach(printDoc);
db.getLastError();
