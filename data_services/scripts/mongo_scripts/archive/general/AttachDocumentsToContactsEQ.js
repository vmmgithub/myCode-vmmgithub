#!/usr/bin/env node

var async = require("async");
var commander = require("commander");
var RestApiInterface = require('./../lib/helpers/RestApi');
var TeamHelper = require('./../lib/helpers/TeamHelper');
var _ = require("underscore");
var fs = require("fs");


commander.version("1.0")
    .option("-h, --host [s]", "Scpecify Host")
    .option("-p, --port [s]", "Scpecify Port")
    .option("-u, --user [s]", "Scpecify User")
    .option("-up, --user-password [s]", "Scpecify User Password")
    .option("-d, --directory [s]", "Scpecify Directory")
    .option("-t, --tenant [s]", "Scpecify Tenant");


commander
    .command("checkDocumentContacts")
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(true).checkDocumentContacts();
    });

commander
    .command("mapDocuments")
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(true).mapDocuments();
    });

commander
    .command("checkExtension")
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(true).checkDocumentExtension(function(){
            console.log("DONE");
        });
    });

commander
    .command("removeExtension")
    .description("execute dryRun")
    .action(function(cmd){
        Scrub(true).removeExtension(function(){
            console.log("DONE");
        });
    });




commander.parse(process.argv);

function Scrub(dryRun){

    var restApi = new RestApiInterface(commander.host,commander.port,commander.user,commander.userPassword);
    var tenant = restApi.setTenant(commander.tenant);
    var contactCollection = restApi.getCollection("core.contacts","core.contact");

    var DOCUMENT_EXTENSION_NAME = "IBReport";
    var DOCUMENTS_DIR = commander.directory.split(",");
    //console.log(DOCUMENTS_DIR);

    var checkDocumentExtension = function(extensions){
        var exists = null;
        _.each(extensions,function(extension,index){
            if(extension.name == DOCUMENT_EXTENSION_NAME){
                exists = index;
                return false;
            }
        });
        return exists;
    }

    var readDocumentDir = function(next,end){
        async.forEachSeries(DOCUMENTS_DIR,function(dir,nextDir){
            fs.readdir(dir,function(err,files){
               next(err,files,dir,nextDir);
	       //console.log(dir);
	       //nextDir();
            });
        },end);
    }

    var getCustomerNumber = function(file){
        var pattern = /_([a-z0-9A-Z]+)_[\W\w]+/;
        pattern.test(file);
        return RegExp.$1;
        //return file.replace(/(\.\w+)$/,"");
    }

    var findContacts = function(document,callback){
        var start = 0;
        var limit = 50;
        var contacts = [];
        var findPart = function(){
            var filter = {
                "extensions.tenant.customerNumber.value": document,
                "type" : {"$in" : ["core.contact/person","core.contact/organization"]}
            };
            //console.log(filter);
            contactCollection.find(filter,{start:start,limit:limit,sort:{"displayName":1}},function(err,records){
                if(err){
                    callback(err);
                }else{
                    if(records && records.length){
                        contacts = contacts.concat(records);
                        if(records.length != limit){
                            callback(null,contacts);
                        }else{
                            start+=limit;
                            findPart();
                        }

                    }else{
                        callback(null,contacts);
                    }
                }
            });
        }
        findPart();
    }

    var uploadDocument = function(filePath,callback){
        var readStream = fs.createReadStream(filePath);
        readStream.on("error",function(err){
          callback(err,null);
        });
        try{
            tenant.attachment(readStream,function(error,response,body){
                if(error){
                    callback(error,null);
                }else{
                    try{
                        //console.log(body);
                        var res = JSON.parse(body);
                        //console.log(res);
                        var file = res[0];
                        callback(null,file);
                    } catch (e){
                        console.log(e);
                        callback(e,null);
                    }
                }
            });
        } catch (e){
            callback(e,null);
        }

    }

    var updateContact = function(contact,uploadedDocument,callback){
        if(!contact.extensions){
            contact.extensions = {}
        }
        if(!contact.extensions.tenant){
            contact.extensions.tenant = {};
        }
        if(!contact.extensions.tenant[DOCUMENT_EXTENSION_NAME]){
            contact.extensions.tenant[DOCUMENT_EXTENSION_NAME] = {type:"core.related.document"}
        }


        contact.extensions.tenant[DOCUMENT_EXTENSION_NAME].value = {
            documentName : decodeURIComponent(uploadedDocument.name),
            link:uploadedDocument.url,
            delete_link : uploadedDocument.delete_url,
            date:uploadedDocument.date
        }

        contactCollection.update(contact,callback);
    }


    return{

        checkDocumentExtension : function(callback){
            var extensionCollection = restApi.getCollection("core.extension.attributes","core.extension.attribute");
            extensionCollection.find({"model":"core.contact"},{},function(err,records){
                if(records && records.length){
                    if(checkDocumentExtension(records[0].extensions) == null){
                        console.log("Extension '" + DOCUMENT_EXTENSION_NAME + "' not found");
                        console.log("Adding Extension '" + DOCUMENT_EXTENSION_NAME + "'");
                        records[0].extensions.push({
                            type : "core.extension.attribute.item",
                            attributeType : "core.related.document",
                            fromMaster : false,
                            name : DOCUMENT_EXTENSION_NAME,
                            cardinality : "1",
                            displayName : "IB Report"
                        });
                        extensionCollection.update(records[0],function(err){
                            if(err){
                                console.log("Error adding extension: " + JSON.stringify(err));
                            }else{

                                console.log("Extension '" + DOCUMENT_EXTENSION_NAME + "' added successfully");
                            }
                            callback();
                        });
                    }else{
                        console.log("Extension: '" + DOCUMENT_EXTENSION_NAME + "' exists");
                        callback();
                    }
                }else{
                    if(err){
                        console.log("Error: " +  JSON.stringify(err));
                    }
                    callback();
                }
            });
        },

        removeExtension : function(callback){
            var extensionCollection = restApi.getCollection("core.extension.attributes","core.extension.attribute");
            extensionCollection.find({"model":"core.contact"},{},function(err,records){
                var index = checkDocumentExtension(records[0].extensions);
                if(index != null){
                    records[0].extensions.splice(index,1);
                    extensionCollection.update(records[0],function(err){
                        if(err){
                            console.log("Error:" + JSON.stringify(err));
                        }
                        callback();
                    })
                }
            });
        },

        checkDocumentContacts : function(){
            readDocumentDir(function(err,files,dir,nextDir){
                async.forEachSeries(files,function(file,next){
                    var customerNumber = getCustomerNumber(file);
                    console.log(customerNumber);
                    //next();
                    findContacts(customerNumber,function(err,contacts){
                        console.log(contacts);
                        next();
                    })
                },function(){
                    nextDir();
                })
            },function(){
                console.log("DONE");
            })
        },

        mapDocuments : function(){
            readDocumentDir(function(err,files,dir,nextDir){
                async.forEachSeries(files,
                    function(file,next){
                        var customerNumber = getCustomerNumber(file);
                        console.log("Customer Nunber: " + customerNumber);
                        findContacts(customerNumber,function(err,records){
                            if(err){
                                console.log("Error found contact via customer number '" + customerNumber + "' :" + JSON.stringify(err));
                                next();
                            }else if(records && records.length){
				var contactList = [];
                                _.each(records,function(record){
				    contactList.push(record.displayName);
				});
				console.log("Contacts for Attaching: " + contactList.join(","));
				//return next();
                                uploadDocument(dir + "/" + file,function(err,documentObj){
                                    if(err){
                                        console.log("Error uploading file '" + DOCUMENTS_DIR + "/" + file + "': " + JSON.stringify(err));
                                        next();
                                    }else{
                                        async.forEachSeries(records,function(record,cb){
                                            updateContact(record,documentObj,function(err){
                                                if(err){
                                                    console.log("Error updating contact:" + JSON.stringify(err));
                                                }else{
                                                    console.log("Contact '" + record.displayName + "' successfully updated");
                                                }
                                                cb();
                                            })
                                        },function(){
                                            next();
                                        })
                                    }
                                })
                            }else{
                                console.log("Error found contact via customer number '" + customerNumber + "'");
                                next();
                            }
                        });
                    },
                    function(){
                        nextDir();
                    }
                )
            },function(){
                console.log("DONE");
            })
        }
    }


}
