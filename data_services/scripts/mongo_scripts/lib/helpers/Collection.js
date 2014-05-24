function Collection(tenant,collectionName,modelName){

    var me = this;
    var collection = tenant.getColl(collectionName);

    me.find = function(filter,options,callback){
        try{
            collection.find(filter, options, function(err, rec) {
                //console.log(rec);
                if(err){
                    callback(err);
                }else if (rec && rec.data && rec.data[modelName] && rec.data[modelName].length) {
                    callback(null, rec.data[modelName]);
                } else {
                    callback(null, null);
                }
            });
        } catch(e){
            callback(e);
        }

    }

    me.create = function(rec,callback){
        try{
            collection.create(rec,callback);
        } catch(e){
            callback(e);
        }
    }

    me.update = function(model,callback){
        try{
            collection.update(model._id,model,callback);
        } catch (e){
            callback(e);
        }

    }

    me.delete = function(model,callback){
        try{
            collection.del(model._id,callback)
        } catch (e){
            callback(e);
        }
    }

    me.getRecord = function(_id){
        return collection.getRecord(_id);
    }

}

module.exports = Collection;