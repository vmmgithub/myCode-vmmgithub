function Collection(tenant, collectionName, modelName) {
    var me = this;
    var collection = tenant.getColl(collectionName);

    me.name = collectionName;

    me.find = function (filter, options, callback) {
        try {
            collection.find(filter, options, function (err, rec) {
                if (err) return callback(err);
                if (rec && rec.data && rec.data[modelName]) return callback(null, rec.data[modelName]);
                callback(null, null);
            });
        } catch (e) {
            callback(e);
        }
    };

    me.findStream = function (filter, options, streamOptions, dataCallback, callback) {
        try {
            collection.findStream(filter, options, streamOptions, function (rec) {
                var r = (rec && rec[modelName] && rec[modelName][0]) || rec;
                if (r && r._id) dataCallback(r);
            }, callback);
        } catch (e) {
            callback(e);
        }
    };

    me.readStream = function (fileName, options, dataCallback, callback) {
        try {
            collection.readStream(fileName, options, function (rec) {
                var r = (rec && rec[modelName] && rec[modelName][0]) || rec;
                if (r && r._id) dataCallback(r);
            }, callback);
        } catch (e) {
            callback(e);
        }
    };

    me.create = function (rec, callback) {
        try {
            collection.create(rec, function (err, rec) {
                if (err) return callback(err);
                if (rec && rec.data && rec.data[modelName]) return callback(null, rec.data[modelName]);
                callback(null, null);
            });
        } catch (e) {
            callback(e);
        }
    };

    me.update = function (model, callback) {
        try {
            collection.update(model._id, model, function (err, rec) {
                if (err) return callback(err);
                if (rec && rec.data && rec.data[modelName]) return callback(null, rec.data[modelName] && rec.data[modelName][0]);
                callback(null, null);
            });
        } catch (e) {
            callback(e);
        }
    };

    me.delete = function (model, callback) {
        try {
            collection.del(model._id, callback)
        } catch (e) {
            callback(e);
        }
    };

    me.getRecord = function (_id) {
        return collection.getRecord(_id);
    };

};

module.exports = Collection;
