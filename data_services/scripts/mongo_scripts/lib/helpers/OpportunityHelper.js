function OpportunityHelper(tenant){

    var coll = tenant.getColl('app.opportunities');

    return{
        find : function(filter,options,callback){
            coll.find(filter, options, function(err, rec) {
                if(err){
                    callback(err);
                }else if (rec && rec.data && rec.data['app.opportunity'] && rec.data['app.opportunity'].length) {
                    callback(null, rec.data['app.opportunity']);
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

module.exports = OpportunityHelper;