var csvHelper = new require("./../lib/helpers/CsvHelper")();
var dryRun = false;
var API = require('./../lib/helpers/api'),
    args = process.argv.slice(2) || [null, null, null, null],
    api = new API(args[0], args[1], args[2], args[3]),
    async = require('async'),
    _ = require('underscore'),
//data = require('./assutf8unix.js').data,
    dellT= api.getTenant('dell'),
    contactColl = dellT.getColl('core.contacts'),
    oppColl = dellT.getColl('app.opportunities'),
    lookupColl = dellT.getColl('core.lookups');



var findLookup = function(data,callback){
    lookupColl.find({ 'group' : 'RelationshipType', 'name' : 'salesRep'}, {limit : 1}, function(err, res) {
        if(err){
            console.log(err);
            callback(err);
        }else{
            var lookup = null;
            if(res && res.data && res.data['core.lookup'] && res.data['core.lookup'].length){
                lookup = res.data['core.lookup'][0];
            }
            callback(null,data,lookup);
        }
    });
}

var applyUpdate = function(data,resRec,callback){

    var salesRepLookup = {
            type : resRec.type,
            key : resRec._id,
            name : resRec.name,
            displayName : resRec.displayName
        },
        DUMMY = { relation : salesRepLookup, target : { key : "dummy"}},
        salesRepRelCache = {};


    async.forEachSeries(data, function(oppToRep, cb) {
        _.each(oppToRep, function(repName, oppName) {
            function getSRR(cb) {
                var srr = salesRepRelCache[repName];
                if (!srr) {
                    contactColl.find({displayName : repName}, {limit : 1, include : ['_id', 'type']}, function(err, res) {
                        if (res && res.data && res.data['core.contact'] && res.data['core.contact'].length) {
                            var resRec = res.data['core.contact'][0];
                            srr = {
                                relation : salesRepLookup,
                                target : {
                                    key : resRec._id,
                                    displayName : resRec.displayName,
                                    type : resRec.type
                                }
                            };
                            salesRepRelCache[repName] = srr;
                            cb(null, srr);
                        } else {
                            console.log("Error: Opp " + oppName + " could not be updated as the sales rep " + repName + " could not be found. Err " + JSON.stringify(err));
                            salesRepRelCache[repName] = DUMMY;
                            cb(null, null);
                        }
                    });
                } else {
                    if (srr == DUMMY) {
                        console.log("Error: Opp " + oppName + " could not be updated as the sales rep " + repName + " could not be found. Err " + JSON.stringify(err));
                        cb(null, null);
                    } else {
                        cb(null, srr);
                    }
                }
            }

            function findOpp(rel, cb) {
                if (rel) {
                    oppColl.find({ displayName : oppName}, { limit : 1, include : ['_id', 'relationships']}, function(err, rec) {
                        if (rec && rec.data && rec.data['app.opportunity'] && rec.data['app.opportunity'].length) {
                            cb(null, rel, rec.data['app.opportunity'][0]);
                        } else {
                            console.log("Error: Opp " + oppName + " could not be found due to error " + JSON.stringify(err));
                            cb(null, null, null);
                        }
                    });
                } else {
                    cb(null, null, null);
                }
            }

            function updateOpp(salesRepRel, oppRec, cb) {
                if (salesRepRel && oppRec) {
                    //var nRels = oppRec.relationships
                    var nRels = _.reject(oppRec.relationships, function (rel) {
                        return ((rel.relation && rel.relation.name == 'salesRep') && (rel.target.key==salesRepRel.target.key));
                    });
                    nRels.push(salesRepRel);
                    var reps = [];
                    _.each(nRels,function(nRel){
                        if(nRel.relation.name == "salesRep"){
                            reps.push(nRel.target.displayName);
                        }
                    })
                    //console.log("Opp " + oppRec.displayName + " salesReps:" + reps.join(", "));
                    if(!dryRun){
                        oppColl.update(oppRec._id, { _id : oppRec._id, relationships : nRels}, function(err, res) {
                            if (err) {
                                console.log("Error: Opp "+ oppName + " could not be updated due to err " + JSON.stringify(err));
                            }else{
                                console.log("Success: " + oppName + " updated with salesRep " + reps.join(", "));
                            }
                            cb(null);
                        });
                    }else{
                        console.log("Success: " + oppName + " updated with salesRep " + reps.join(", "));
                        //console.log("Success: " + oppName + " updated with salesRep " + salesRepRel.target.displayName);
                        cb(null);
                    }
                } else {
                    cb(null);
                }
            }
            async.waterfall([getSRR, findOpp, updateOpp], cb);
        });
    }, function() {
        callback();
        console.log("All Done");
    })
}

csvHelper.read("./csv/Sweden_UTF-8.csv", function (data) {
    async.waterfall([
        function(callback){
            findLookup(data,callback)
        },
        function(data,rel,callback){
            applyUpdate(data,rel,callback)
        }
    ],function(){
        console.log(["Done"]);
    });
});
