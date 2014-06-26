#!/usr/bin/env node

var async = require("async");
var _ = require("underscore");
var commander = require("commander");
var csvHelperInstance = require("./../lib/helpers/CsvHelper");
var RestApiInterface = require('./../lib/helpers/RestApi');

commander.version("1.0")
    .option("-h, --host [s]", "Specify host")
    .option("-s, --port [s]", "Specify Port")
    .option("-u, --user [s]", "Specify User")
    .option("-p, --password [s]", "Specify User Password")
    .option("-t, --tenant [s]", "Specify Tenant")
    .option("-f, --file [s]", "Specify File");

commander
    .command("assign")
    .description("Assigns the batch quarter for each offer")
    .action(function (cmd) {
        Scrub(true).executeBatchQuarterUpdate();
    });

commander.parse(process.argv);

function Scrub() {

    var restApi = new RestApiInterface(commander.host, commander.port, commander.user, commander.password);
    restApi.setTenant(commander.tenant);

    var offerCollection = restApi.getCollection("app.offers", "app.offer");
    var csvHelper = new csvHelperInstance();

    var findOffer = function (displayName, callback) {
        offerCollection.find({
            displayName: displayName
        }, {
            start: 0,
            limit: 1
        }, callback);
    };
  
  var updateOfferBatchQuarter = function (offer, batchQuarter, callback) {
        if (!offer.extensions) {
            offer.extensions = {};
        }
        if (!offer.extensions.tenant) {
            offer.extensions.tenant = {};
        }
        offer.extensions.tenant.batchQuarter = {
            type: "string"
        };

        {
            offerCollection.update(offer, callback);
        } 

    };
   
    return {
       executeBatchQuarterUpdate: function () {
            csvHelper.readAsObj(commander.file, function (data) {
                async.forEachSeries(data,
                    function (csvRecord, callback) {
                        findOffer(csvRecord["name"], function (err, offers) {
                            if (err || !offers || offers.length != 1) {
                                console.log("Error finding offer '" + csvRecord["name"] + "': '" + JSON.stringify(err));
                                return callback();
                            } else if (offers && offers.length) {
                                updateOfferBatchQuarter(offers[0],csvRecord["batchQuarter"],function(err){
                                    if (err || !offers || offers.length != 1) {
                                console.log("Error finding offer '" + csvRecord["name"] + "': '" + JSON.stringify(err));
                                return callback();
                                    } else {
                                        var msg = "Success: Offer '" + csvRecord["name"] + "' successfully updated with '" + csvRecord["batchQuarter"] + "' batchQuarter'";
                                        console.log(msg);
                                    }
                                    callback();
                                });
                            } else {
                                console.log("'Error: offer: '" + csvRecord["name"]+ "' doesn't exist'");
                                callback();
                            }
                        });
                    },
                    function () {
                        console.log("DONE");
                    }
                );
            });
        }
    };
}
