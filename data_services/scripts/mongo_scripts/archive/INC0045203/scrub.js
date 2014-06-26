var API = require('./../lib/helpers/api'),
    Collection = require("./../lib/helpers/Collection"),
    async = require("async"),
    _ = require("underscore"),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    tenant = api.getTenant('guidance'),
    flowCollection = new Collection(tenant,"core.flows","core.flow")


var Scrub = function(){

    var dryRun = true;
    var filter = {"systemProperties.tenant":"guidance","targetCollection.name":"app.bookings"};


    var pendingToHoldFLowDef = { "type": "core.flow.event",
        "condition": "",
        "actionType": {
            "name": "user"
        },
        "action": {
            "name": "holdBooking"
        },
        "triggerState": {
            "type": "core.flow.state",
            "name": "pending",
            "displayName": "Pending",
            "order": 1
        },
        "nextState": {
            "type": "core.flow.state",
            "name": "onHold",
            "displayName": "On Hold",
            "order": 2
        },
        "sideEffects": [
            {
                "notify": {
                    "subjectTemplateName": "booking_hold_subject",
                    "bodyTemplateName": "booking_hold_body",
                    "recipients": [
                        "salesRep",
                        "requestedBy" ]
                }
            }
        ],
        "parameters": []
    }

    var findBookingFlowDefinition = function(callback){
        flowCollection.find(filter,{},callback);
    }

    var updateBookingDefinition = function(record){
        return function(callback){
            record.events.push(pendingToHoldFLowDef);
            if(dryRun == false){
                flowCollection.update(record,function(err,records){
                    if(err){
                        console.log("Error: can't update booking flow definition due error: " + JSON.stringify(err));
                    }else{
                        console.log("Success: " + record.name + " successfully updated");
                    }
                    callback(err,record);
                });
            }else{
                console.log("DryRun Success: " + record.name + " successfully updated");
                callback(null,record);
            }

        }
    }



    return{
        execute : function(dryRunMode){
            if(dryRunMode != undefined){
                dryRun = dryRunMode;
            }
            findBookingFlowDefinition(function(err,records){
                if(err){
                    console.log("Error: can't find Booking flow definition due error: " + JSON.stringify(err))
                }else if(records && records.length){
                    var funcs = [];
                    _.each(records,function(record){
                        funcs.push(updateBookingDefinition(record));
                    });
                    async.series(funcs,function(err,results){
                        console.log("DONE");
                    })
                }else{
                    console.log("Error: Bookings definitions are empty");
                }

            });
        }

    }
}

new Scrub().execute(false);



