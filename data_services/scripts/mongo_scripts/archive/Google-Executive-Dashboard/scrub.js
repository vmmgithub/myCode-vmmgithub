var API = require('./../lib/helpers/api'),
    Collection = require("./../lib/helpers/Collection"),
    async = require("async"),
    _ = require("underscore"),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    tenant = api.getTenant('google'),
    tenantConfig = new Collection(tenant,"core.tenant.configs","core.tenant.config");



var Scrub = function(){

    var dryRun = true;

    var reportConfig = {
        "type": "core.tenant.config",
        "appDomain": "gooddataConfig",
        "name": "reportConfig",
        "displayName": "Report Configuration",
        "description": "GoodData Report Configuration",
        "configs": [
            {
                "name": "project",
                "data": {
                    "value": "jivfngon23kow6cv6m1a9t2hy8f0rn5w"
                }
            },
            {
                "name": "dashboard",
                "data": {
                    "value": "42350"
                }
            }
        ],
        "systemProperties": {
            "tenant": "google"
        }
    }

    var findGoodDataConfig = function(callback){
        tenantConfig.find({"name":"reportConfig","appDomain":"gooddataConfig"},{},function(err,records){
            if(err){
                callback(err,null);
            }else if(records && records.length){
                callback(null,records[0]);
            }else{
                callback(null,null);
            }
        })
    }

    var saveConfig = function(record,callback){
        if(record){
            record.configs = reportConfig.configs;
            tenantConfig.update(record,callback);
        }else{
            tenantConfig.create(reportConfig,callback)
        }
    }

    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            async.waterfall([
                findGoodDataConfig,
                function(record,callback){
                    saveConfig(record,callback);
                }
            ],function(err,results){
                console.log("DONE");
            })

        }

    }
}

new Scrub().execute(false);



