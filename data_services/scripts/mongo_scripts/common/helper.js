//var tenant = "dell";

var _id = function(doc) {
return doc._id.valueOf()
}

var id = function (doc) {
    return doc.externalIds && doc.externalIds[0] && doc.externalIds[0].id;
};

var isoDate = function (dt) {
    if (dt && dt.getFullYear)
       return (dt.getFullYear() + '-' + (dt.getMonth()+1) + '-' + dt.getDate());
    else 
        return '';
};

var curr = function(curr, normalized, code) {
    if (!curr) return;
    var c = normalized ? curr.normalizedAmount : curr;
    if (!code) 
	return toFixed(c && c.amount);
    else 
        return ('' + toFixed(c && c.amount) + ' ' + (c && c.code && c.code.displayName || c.code.name)); 
}

var extValue = function (doc, name, isMaster) {
  var e = isMaster ? 'master': 'tenant'; 
  if (doc && doc.extensions && doc.extensions[e] && doc.extensions[e][name] && doc.extensions[e][name].value) {
    if (doc.extensions[e][name].type != 'date') 
      return (doc.extensions[e][name].value.displayName || doc.extensions[e][name].value.name || doc.extensions[e][name].value);
    else
      return isoDate(doc.extensions[e][name].value);
  } else {
    return '';
  }
};

var getRelKey = function (doc, name, i) {
  if (!i) i = 0;
  return (doc && doc.relationships && doc.relationships[name] && doc.relationships[name].targets && doc.relationships[name].targets[i] && doc.relationships[name].targets[i].key);
};

var getRel = function (doc, name, i) {
  if (!i) i = 0;
  return (doc && doc.relationships && doc.relationships[name] && doc.relationships[name].targets && doc.relationships[name].targets[i]);
};

var getRelKeys = function (doc, name) {
  if(doc && doc.relationships && doc.relationships[name] && doc.relationships[name].targets && doc.relationships[name].targets.length > 0 ) {
    var ids = [];
    doc.relationships[name].targets.forEach(function(t) {
      ids.push(t.key);
    });
    return ids;
  }
};

var getLookup = function(tenant, name, core, group) {
        var coll = core ? "core.lookups": "app.lookups";
        var f = {"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), displayName: name};
	if (group) f.group = group;

        var lkp = db[coll].findOne(f);
        if (!lkp) 
                return lkp;
        else 
                return {name: lkp.name, displayName: lkp.displayName, type: lkp.type, key: lkp._id.valueOf()};
};

var getLookupWithValue = function(tenant, name) {
        var lkp = db.app.lookups.findOne({"systemProperties.tenant": tenant, "systemProperties.expiredOn": ISODate("9999-01-01"), displayName: name});
        if (lkp) 
            return {name: lkp.name, displayName: lkp.displayName, type: lkp.type, key: lkp._id.valueOf(), value: lkp.value};
};

var getNestedRel = function (doc, name, subName) {
  var par= getRel(doc, name);
  return getRel(par, subName);
};

var getRelName = function (doc, name) {
  var rel = getRel(doc, name);
  return (rel && rel.displayName) || '';
}

var getNestedRelName = function (doc, name, subName) {
  var rel = getNestedRel(doc, name, subName);
  return (rel && rel.displayName) || '';
}

var printVals = function(arr) {
        var str = "";
        arr.forEach(function(a) {
                str += a + "\t";
        });
        print (str);
};

function checkPrecision(val, base) {
	val = Math.round(Math.abs(val));
	return isNaN(val)? base : val;
}

var unformat = function(value, decimal) {

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

var toFixed = function(value, precision) {
	precision = checkPrecision(2, 2);
	var power = Math.pow(10, precision);

	// Multiply up by precision, round accurately, then divide and use native toFixed():
	return parseFloat((Math.round(unformat(value) * power) / power).toFixed(precision));
};

