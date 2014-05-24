load(file);
load('../common/helper.js');

var tenant="dell";
var coll = "app.assets";

var i = 0;
var updated = 0;

values.forEach(function(v) {
        if (i++ % 1000 == 0) print('[' + ISODate()+ '] Setting done with for ' + i + ' records and updated ' + updated);
        if (!v.uid || v.uid == 'undefined') return;
        var dt = ISODate(v.value);

db[coll].find({ 'externalIds.id': v.uid }, { _id : 1, 'systemProperties.expiredOn' : 1, 'extensions.tenant.eosDate' : 1 }).hint('extDl').forEach(function(rec) {

if(rec.systemProperties.expiredOn.valueOf() == ISODate('9999-01-01').valueOf() && (!rec.extensions || !rec.extensions.tenant || !rec.extensions.tenant.eosDate || !rec.extensions.tenant.eosDate.value || rec.extensions.tenant.eosDate.value != ''))  {
                db[coll].update({_id : rec._id }, {$set: {'extensions.tenant.eosDate': {type: 'date', value: dt }}}, false, true);
                updated++;
        }
})
});
db.getLastError();
print("Done " + i + ' ' + updated);
