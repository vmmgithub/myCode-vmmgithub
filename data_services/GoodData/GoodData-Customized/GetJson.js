var url = process.argv[3];
var tenent_args = process.argv[2];
var entity_args = process.argv[4];
var query_args = process.argv[5];
var folderPath_args = process.argv[2];
var fileName_args = process.argv[6];
var userName_args = process.argv[7];
var passwordone_args = process.argv[8];
var method_args = process.argv[9];
console.log("Query : "+query_args);
console.log(" Url: "+url+" tenent: "+tenent_args+" entity_args : "+entity_args+" FileName"+ fileName_args);
exportJson(url,tenent_args,entity_args,query_args,folderPath_args,fileName_args,userName_args,passwordone_args,method_args);
//exports.exportJson = function exportJson(serverName,tenant,entity,query,folderPath,fileName,userName,passWord){
function exportJson(serverName,tenant,entity,query,folderPath,fileName,userName,passWord,method){
var fs = require("fs");
var request = require('request');
var https = require('https');
var csv = require('./fileRead.js');
var totalBytes=0;

var stream = fs.createWriteStream(folderPath+'/Json'+'/'+fileName, {flags: 'w'});
var logStream = fs.createWriteStream(folderPath+'/'+'Logs'+'/AppLog-'+new Date().toISOString().slice(0,10)+'.log', {flags: 'a'});
var errorLogStream = fs.createWriteStream(folderPath+'/'+'/Logs'+'/ErrorLog_'+new Date().toISOString().slice(0,10)+'.log', {flags: 'a'});
        var authentication = '{"username":"'+userName+'@'+tenant+'.com","password":"'+passWord+'"}';
        var start=new Date();
        try
        {

                var login = request.post({
                key:    fs.readFileSync('config-t2.key'),
                cert:   fs.readFileSync('config-t2.crt'),
                requestCert:        true,
                rejectUnauthorized: false,
                  headers: {
                                        'Content-Length': Buffer.byteLength(authentication),
                                        'content-type': 'application/json; charset=UTF-8',
                                        'Cookie':'avalon.sid=s%3AiSXLEBq2ThdI82VLHmw1E5KC.N%2Fczt5zDZVInnBD0zIFonCxctzsH6e4xHtPVOX90pSE'
                                        },
                                        port: 443,
                  url:     'https://'+serverName+'/login.json',
                  body:    authentication
                }, function(error, response, body){
                        if(response.statusCode ==200){

                                        var options = {
                                                key:    fs.readFileSync('config-t2.key'),
                                                cert:   fs.readFileSync('config-t2.crt'),
                                                requestCert:        true,
                                                rejectUnauthorized: false,
                                                host:serverName,
                                                path:'/rest/api/'+tenant+'/'+entity+'::'+method,
                                                method:'POST',
                                                headers:{
                                                                'Content-Length': Buffer.byteLength(query),
                                                                'Cookie':response.headers["set-cookie"][0].split(';')[0],
                                                                'Connection':'keep-alive',
                                                                'Referer': 'https://'+serverName+'/tests/test.html',
                                                                'content-type': 'application/json; charset=UTF-8',
                                                                'tenant': tenant
                                                                }
                                                };

                                        var request = https.request(options,function(response){
                                                                        response.on("data",function(chunk){
                                                                                totalBytes+= chunk.length;
                                                                                stream.write(chunk.toString());
                                                                                });
                                                                        response.on("end",function(){
                                                                                                var textAppend = "===============================================================\n";
                                                                                                textAppend+="Status Report for Json Stream - "+entity+"\n";
                                                                                                textAppend+= "===============================================================\n";
                                                                                                textAppend+="Json Export Started :"+start;
                                                                                                textAppend+="\nJson Export Ended :"+new Date();
                                                                                                textAppend+="\nTime Taken :"+(new Date() - start)/1000+" seconds\n";
                                                                                                textAppend+="Number of bytes reached:"+totalBytes+'\n';
                                                                                                textAppend+= "===============================================================\n";
                                                                                                        logStream.write(textAppend+'\n');
                                                                                                        csv.convertToCsv(folderPath+'/Json'+'/'+fileName,folderPath+'/CSV'+'/'+fileName.replace('.json',''),entity,folderPath+'/'+'Logs/AppLog-'+new Date().toISOString().slice(0,10)+'.log',fileName.replace('.json',''),tenant)

                                                                                                });
                                                                        response.on("error",function(err){
                                                                                                        errorLogStream.write("Error from Json Streaming :"+err+"\n\n");
                                                                                                });
                                                                        });
                                                                        request.setTimeout(1000*60000*6000*1000*1000,function(){console.log("wait Over")});
                                                                request.write(query);
                                                                request.end();

                        }
                        else{
                        errorLogStream.write("Error from Streaming :Error Occured while connecting server.\nPlz check username and password\n\n");
                        }
                });
                }
                catch(err)
                {
                        errorLogStream.write("Error From Json Stream :"+err+"\n\n");
                }
}

