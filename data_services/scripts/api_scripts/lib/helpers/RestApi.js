var AvalonRestApi = require("./api");
var AvalonCollection = require("./Collection");

module.exports = function () {

    function RestApi(host, port, user, password) {

        var me = this;
        var apiInterface = new AvalonRestApi(host, port, user, password);
        var tenantApi = undefined;
        var collectionsCache = {};

        me.setTenant = function (tenant) {
            me.reset();
            tenantApi = apiInterface.getTenant(tenant);
            return tenantApi;
        }

        me.getCollection = function (collectionName, modelName) {
            var collection = undefined;
            if (tenantApi != undefined) {
                if (!collectionsCache[collectionName]) {
                    collectionsCache[collectionName] = new AvalonCollection(tenantApi, collectionName, modelName);
                }
                collection = collectionsCache[collectionName];
            }
            return collection;
        }

        me.reset = function () {
            collectionsCache = {};
            tenantApi = undefined;
        }
    }

    return RestApi;
}();
