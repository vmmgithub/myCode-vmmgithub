var _ = require("underscore");


var ExtensionHelper = {

    getExtension : function(model,category,name){
        var extension = undefined;
        _.each(model.extensions,function(items,extCategory){
            if(category == extCategory){
                _.each(items,function(value,extName){
                    if(extName == name){
                        extension = value;
                    }
                })
            }
        });
        return extension;
    }

}

module.exports = ExtensionHelper;