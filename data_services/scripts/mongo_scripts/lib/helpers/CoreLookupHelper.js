function CoreLookupHelper(tenant){

    var coll = tenant.getColl('core.lookups');

    return{
        find : function(filter,options,callback){
            coll.find(filter, options, function(err, res) {
                if(err){
                    callback(err);
                }else{
                    var lookups = null;
                    if(res && res.data && res.data['core.lookup'] && res.data['core.lookup'].length){
                        lookups = res.data['core.lookup'];
                        callback(null,lookups);
                    }else{
                        callback(null,null);
                    }
                }
            });
        }
    }
}

module.exports = CoreLookupHelper;