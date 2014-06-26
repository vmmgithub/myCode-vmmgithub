var csvHelper = new require("./../lib/helpers/CsvHelper")();
var _ = require("underscore");
var async = require("async");
var fs = require("fs");
var lineReader = require("line-reader");


var logFiles = ["scrub1.log","scrub1-1.log","scrub1-2.log","scrub2.log","scrub2-2.log","scrub2-3.log","scrub3.log","scrub3-3.log","scrub3-4.log","scrub4.log","scrub4-4.log","scrub4-5.log"];
//var logFiles = ["scrub1.log"];


var getAllOpportunities = function(callback){
    console.log("Get All Opportunities")
    var oppNames = [];
    csvHelper.readAsObj("./csv/opportunities.csv",function(records){
        _.each(records,function(rec){
            if(oppNames.indexOf(rec["Name"]) == -1){
                oppNames.push(rec["Name"]);
            }
        });
        callback(null,oppNames);
    });
}

var getOpportunityName = function(line){
    var result = undefined;
    var successPattern = /Success/;
    var oppNamePattern  = /opportunity '([\w\W]+)' successfully/;
    if(successPattern.test(line)){
        oppNamePattern.test(line);
        result = RegExp.$1;
    }
    return result;
}

var getOppFromLogFile = function(file,callback){
    var oppNames = [];
    lineReader.eachLine(file,function (line) {
       var oppName = getOpportunityName(line);
        if(oppName){
            oppNames.push(oppName);
        }
    }).then(function () {
        callback(null,oppNames);
    });
}


var getSuccessOpportunities = function(callback){
    var successOppNames = [];
    var asyncConfig = {};
    _.each(logFiles,function(file){
        asyncConfig[file] = function(callback){
            getOppFromLogFile("log/" + file,callback);
        }
    });
    async.series(asyncConfig,function(err,results){
        _.each(results,function(oppNames){
            _.each(oppNames,function(oppName){
                if(successOppNames.indexOf(oppName) == -1){
                    successOppNames.push(oppName)
                }
            })
        });
        console.log["getSuccessOpportunities"];
        callback(null,successOppNames);
    });
}


async.series({
    all : function(callback){getAllOpportunities(callback)},
    success : function(callback){getSuccessOpportunities(callback)}
},function(err,results){

    var notProcessed = [];
    _.each(results.all,function(oppName){
        if(results.success.indexOf(oppName) == -1){
            notProcessed.push(oppName);
        }
    });
    console.log(results.all.length);
    console.log(results.success.length);
    //console.log(notProcessed);
});

//csvHelper.readAsObj("./csv/opportunities.csv",function(records){
//    var duplicates = [];
//    var oppNames = [];
//    _.each(records,function(rec){
//        if(oppNames.length == 0){
//            oppNames.push(rec["Name"]);
//        }else{
//            if(oppNames.indexOf(rec["Name"]) == -1){
//                oppNames.push(rec["Name"]);
//            }else{
//                duplicates.push(rec["Name"]);
//            }
//        }
//    });
//    console.log(duplicates.length);
//})

//getSuccessOpportunities();

//var buildOpportunityNamesFromLog = function(){
//
//}

