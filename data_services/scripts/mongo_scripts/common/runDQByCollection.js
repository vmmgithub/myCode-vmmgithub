//var tenant = 'ibm';
//var coll = 'app.assets';
//var captureDetails = false;

load('./helper.js');
load('./underscore.js');

var base = {
    'systemProperties.tenant': tenant,
    'systemProperties.expiredOn': ISODate('9999-01-01'),
};

// 0.0 Limits & base information
var limitYears = 50;

var tooFarAhead = ISODate();
tooFarAhead.setFullYear(tooFarAhead.getFullYear() + limitYears);

var tooFarBack = ISODate();
tooFarAhead.setFullYear(tooFarAhead.getFullYear() - limitYears);

var meta = {
    'app.assets': {}
};

// 0.1 Helper functions
var getAllowedValues = function(model, propertyPath) {
    var clc = db.core.lookup.configs.findOne(_.extend({}, base, {model: model, propertyPath: propertyPath}));
    var allowedValues = [];

    if (clc && clc.srcCollection && clc.filter) {
        var as = db[clc.srcCollection].find(_.extend({}, base, clc.filter), {name: 1, type: 1, displayName: 1, value: 1}).toArray();
        if (as && !_.isEmpty(as)) allowedValues = as;

        _.each(allowedValues, function(av) { av.key = av._id.valueOf(); delete av._id;});
    }        
    return allowedValues;
};

var getDeepValue = function(obj, path) {
    if (!path || !obj) return;

    var ret = obj;
    _.each(path.split('.'), function(key) {
        ret = ret && ret[key];
    });

    return ret;
};

var flatten = function(arr, keyPath) {
    if (!arr || _.isEmpty(arr)) return;

    var temp = {};
    _.each(arr, function(ele) {
        var name = getDeepValue(ele, keyPath);
        temp[name] = ele;
    });

    return temp;
};

// 1.1 Find the model based on the collection
var model = db.core.metadata.collections.findOne(_.extend({}, base, {'systemProperties.tenant': 'master', name: coll})).model;

// 1.2 Find out all the properties and subTypes
var models = db.core.metadata.models.find(_.extend({}, base, {'systemProperties.tenant':  {$in: [tenant, 'master']}, name: model}));

var props = {};
var subTypes = [];

models.forEach(function(m) {
    if (m.model && m.model.properties) props = _.extend(props, m.model.properties);
    if (m.subTypes && !_.isEmpty(m.subTypes)) subTypes = _.union(subTypes, m.subTypes);
});

// 1.3 Get allowable values for base attributes
_.each(_.keys(props), function(propName) {
    if (props[propName].type == "lookup") props[propName].allowedValues = getAllowedValues(model, propName);
});

// 1.4 Find out the applicable extensions
var extensions = db.core.extension.attributes.findOne(_.extend({}, base, {model: model}));
extensions = (extensions && extensions.extensions) || [];

// 1.5 Read up all the lookup configs & allowable values
_.each(extensions, function(ext) {
    if (ext.attributeType == 'lookup') {
        var propertyPath = ext.fromMaster ? ('extensions.master.' + ext.name) : ('extensions.tenant.' + ext.name);
        ext.allowedValues = getAllowedValues(model, propertyPath);
    }
});

// 1.6 Find out the applicable relaitonships
var relations = db.core.relationship.types.findOne(_.extend({}, base, {source: model}));
relations = (relations && relations.relations) || [];

// 1.7 Prep all the relevant information into appropriate subType containers
if (!_.isEmpty(subTypes)) {
    _.each(subTypes, function(subType) {
        subType.type = model + '/' + subType.name;

        var r = db.core.relationship.types.findOne(_.extend({}, base, {source: subType.type}));
        if (r && r.relations) subType.relations = _.union(relations, r.relations);

        subType.props = props;
        subType.model = model;
        subType.extensions = extensions;
    });
} else {
    subTypes.push({
        type: model,
        props: props,
        model: model,
        relations: relations,
        extensions: extensions
    });
}

// 1.8 Stuff all the introspection info into easily accessible object
_.each(subTypes, function(subType) { 
    _.each(subType.relations, function(rel) { rel.relations = flatten(rel.relations, 'name.name');});
    subType.relations = flatten(subType.relations, 'name.name');
    subType.extensions = flatten(subType.extensions, 'name');
});
var metadata = flatten(subTypes, 'type');


// 2.0 Start actual recon
var stats = {};

var incStats = function(doc) {
    var type = doc.type;
    if (!stats[type]) stats[type] = {count: 0, amount: 0, targetAmount: 0};
    stats[type].count++;
};

var addError = function(doc, key) {
    var type = doc.type;
    if (!stats[type][key]) stats[type][key] = 0;
    stats[type][key]++; 

    if (captureDetails) {
        if (!stats[type].details) stats[type].details = {};
        if (!stats[type].details[key]) stats[type].details[key] = [];
        stats[type].details[key].push(doc._id.valueOf());
    }
};

var scanNumber = function(doc, fieldName, field) {
    if (!field || !_.isNumber(field)) addError(doc, fieldName + '.invalidNumber');    
};

var scanBoolean = function(doc, fieldName, field) {
    if (!field || !_.isBoolean(field)) addError(doc, fieldName + '.invalidBoolean');    
};

var scanDate = function(doc, fieldName, field) {
    if (!_.isDate(field)) addError(doc, fieldName + '.invalidDate');
    if (field && field > tooFarAhead) addError(doc, fieldName + '.tooFarAhead');
    if (field && field < tooFarBack) addError(doc, fieldName + '.tooFarBack');
};

var scanLookups = function(doc, fieldName, field, allowedValues) {
    if (!field || !field.name)  addError(doc, fieldName + '.missingLookup');
    if (field && field.name && (!field.displayName || !field.key))  addError(doc, fieldName + '.unresolvedLookup');
    if (field && field.name && ! _.find(allowedValues, function(av) { return av.name ==  field.name})) addError(doc, fieldName + '.unexpectedLookup');
    if (field && field.displayName && ! _.find(allowedValues, function(av) { return av.displayName ==  field.displayName})) addError(doc, fieldName + '.differentLookupName');
};

var scanAmount = function(doc, fieldName, field) {
    if (!field.amount || !_.isNumber(field.amount)) addError(doc, fieldName + '.missingAmount');
    if (field.amount < 0) addError(doc, fieldName + '.negativeAmount');

    if (!field.code || !field.code.name) addError(doc, fieldName + '.missingCurrencyCode');
    if (!field.normalizedAmount || !field.normalizedAmount.code || !field.normalizedAmount.code.name || !field.normalizedAmount.amount) addError(doc, fieldName + '.missingNormalizedAmount');
    if (field.code && field.code.name && !field.code.displayName) addError(doc, fieldName + '.unresolvedCurrencyCode');
    if (field.code && field.code.name && field.normalizedAmount && field.normalizedAmount.code && field.normalizedAmount.code.name 
        && field.normalizedAmount.code.name != field.code.name && field.amount == field.normalizedAmount.amount)
        addError(doc, fieldName + '.improperlyConvertedAmount');
    // Check for unexpected currency code
    //if (field.code && field.code.name && !_.find(def.allowedValues, function(av) { return av.name == field.code.name})) addError(doc, fieldName + '.unexpectedCurrencyCode');
    //Check if normalizedCurrency is different from baseCurrency
};

var scanAttribute = function(doc, attributeType, fieldName, field, allowedValues) {
    if (attributeType == 'lookup') scanLookups(doc, fieldName, field, def.allowedValues);
    if (attributeType == 'date' || attributeType == 'Date' ) scanDate(doc, fieldName, field);
    if (attributeType == 'number') scanNumber(doc, fieldName, field);
    if (attributeType == 'currency' || attributeType == 'core.currency') scanAmount(doc, fieldName, field);
    if (attributeType == 'boolean') scanBoolean(doc, fieldName, field);    
}

var scan = function(doc) {
    var type = doc.type;
    var m = metadata[type] || {extensions: {}, relations: {}, props: {}, model: type};

    incStats(doc);

    if (doc.relationships) {

        // Scan existing relationships for valid references
        _.each(_.keys(doc.relationships), function(relName) {
            var rel = doc.relationships[relName];
            if (!rel.targets || _.isEmpty(rel.targets)) return;

            rel.targets.forEach(function(target) {
                if (target.key && target.displayName && target.type) return;
                else addError(doc, 'relationship.' + relName + '.unresolved');
            });
        });
    }

    if (doc.extensions) {
        if (!doc.extensions.master) doc.extensions.master = {};
        if (!doc.extensions.tenant) doc.extensions.tenant = {};

        // Scan extensions
        _.each(_.union(_.keys(doc.extensions.master), _.keys(doc.extensions.tenant)), function(extName) {
            var ext = doc.extensions[extName];
            var def = m.extensions[extName];

            // Type checks
            if (!ext) return;
            if (!def) addError(doc, 'extension.' + extName + '.unknown');
            if (def && def.attributeType != ext.type) addError(doc, 'extension.' + extName + '.mismatchedType');
            if (!ext.value) addError(doc, 'extension.' + extName + '.missingValue');
            
            scanAttribute(doc, def && def.attributeType, 'extension.' + extName, ext.value, def && def.allowedValues);
        });        
    }

    //Scan properties
    _.each(_.keys(m.props), function(propName) {
        if (!doc[propName]) return;
        var def = m.props[propName];

        scanAttribute(doc, def.type, 'prop.' + propName, doc[propName], def.allowedValues);
    });
};

db[coll].find(_.extend({}, base, {'systemProperties.qRank': 1})).readPref('secondary').addOption(DBQuery.Option.noTimeout).forEach(scan);
db.getLastError();
printjson(stats);
print('Done');


