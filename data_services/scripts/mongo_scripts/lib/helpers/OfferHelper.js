function OfferHelper(tenant){

    var coll = tenant.getColl('app.offers');

    return{
        find : function(filter,options,callback){
            coll.find(filter, options, function(err, rec) {
                if(err){
                    callback(err);
                }else if (rec && rec.data && rec.data['app.offer'] && rec.data['app.offer'].length) {
                    callback(null, rec.data['app.offer']);
                } else {
                    callback(null, null);
                }
            });
        },
        update : function(_id,update,callback){
            coll.update(_id,update, function(err, res) {
                if (err) {
                    callback(err);
                }else{
                    callback(null,res)
                }
            });
        }
    }
}

module.exports = OfferHelper;