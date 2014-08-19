#!/usr/bin/env node

var serverName = "prod02dl-int.ssi-cloud.com";
var tenant = "cisco";
var collection = "app.dataload.configs";
var query = '{"params": {"stream":"true", "limit":-1}}';
var userName = "bill.moor";
var passWord = "passwordone";
var method = "find";

var _ = require("underscore");
var async = require("async");
var fs = require("fs");
var request = require('request');
var https = require('https');
var totalBytes = 0;

var authentication = '{"username":"' + userName + '@' + tenant + '.com","password":"' + passWord + '"}';
var start = new Date();
var stream = fs.createWriteStream('fs.tmp', {flags: 'w'});

try {

    var loginPayload = {
        requestCert: true,
        rejectUnauthorized: false,
        headers: {
            'Content-Length': Buffer.byteLength(authentication),
            'content-type': 'application/json; charset=UTF-8',
        },
        port: 443,
        url: 'https://' + serverName + '/login.json',
        body: authentication
    };
    console.log(loginPayload);
    var login = request.post(loginPayload, function (error, response, body) {
        if (response.statusCode == 200) {

            var options = {
                requestCert: true,
                rejectUnauthorized: false,
                port: 443,
                host: serverName,
                method: 'POST',
                headers: {
                    'Connection': 'keep-alive',
                    'content-type': 'application/json; charset=UTF-8',
                    'Referer': 'https://' + serverName + '/tests/test.html',
                    'tenant': tenant,
                    'Cookie': response.headers["set-cookie"][0].split(';')[0],
                    'Content-Length': Buffer.byteLength(query),
                },
                path: '/rest/api/' + tenant + '/' + collection + '::' + method,
            };

            var request = https.request(options, function (response) {
                response.on("data", function (chunk) {
                    totalBytes += chunk.length;
                    stream.write(chunk.toString());
                });
                response.on("end", function () {
                    console.log("Done");
                });
                response.on("error", function (err) {
                    console.log("Error from Json Streaming :" + err);
                });
            });
            request.setTimeout(1000 * 60000 * 6000 * 1000 * 1000, function () {
                console.log("Timeout error")
            });
            request.write(query);
            request.end();

        } else {
            console.log("Error from Streaming :Error Occured while connecting server.\nPlz check username and password\n\n");
        }
    });
} catch (err) {
   console.log("Error From Json Stream :" + err + "\n\n");
}
