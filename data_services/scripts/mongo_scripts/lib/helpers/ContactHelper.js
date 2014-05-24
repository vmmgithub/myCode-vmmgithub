function ContactHelper(tenant){

    var coll = tenant.getColl('core.contacts');

    return{
        find : function(filter,options,callback){
            coll.find(filter, options, function(err, res) {
                if(err){
                    callback(err,null);
                } else if (res && res.data && res.data['core.contact'] && res.data['core.contact'].length) {
                    callback(null,res.data['core.contact']);
                } else {
                    callback(null, null);
                }
            });
        }
    }
}

module.exports = ContactHelper;