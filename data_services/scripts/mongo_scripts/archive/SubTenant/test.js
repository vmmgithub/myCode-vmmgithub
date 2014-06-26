var fs = require("fs");
var api = require("./Api.js");
var Api = new api({serverName:'config-t2.ssi-cloud.com',tenant:'bluecoat',userName:'bruce.lewis'});
var org,acntHead,fileData='',temp='';//reseller_role,distributor_role,admin_role;
var records,count = 0;
var fileName = process.argv[2];
var usersDone ='';
var unfinishedOrg = '';
console.log(fileName)
// Preparing Data to Start Engine...
//Input Formatt :- {"tenant":"abc","members":[{"email":"trail","name":"test moor"}]}
// Starting the Engine...
//inFile is the hardcoded file name
Api.login(function(data){
	records = fs.readFileSync(fileName.toString().trim());
	createTenantAndMembers(JSON.parse(records.toString().replace('""','"').trim().replace('undefined','')),function(){
		//count++;
	});
});

//{"tenant":"abc","members":[{"email":"trail","name":"test moor"}]}
function createTenantAndMembers (tenantObj,callback) {
	fs.appendFile('log.txt',"\n\rFile Name : "+fileName, function (err) {});
	console.log(tenantObj)
	var getOrg = {"filter":{"displayName":"Test","type":"core.contact/organization"},"params":{"stream":true,"limit":1}};
	getOrg.filter.displayName = tenantObj["tenant"];
	var getTeam = {"filter":{"displayName":"Account Head"},"params":{"stream":true,"limit":1}};
	if(falOrg.indexOf(tenantObj["tenant"])==-1){
	Api.restApiCall("core.teams","::find",getTeam,function(dataa){
		if(dataa){
		acntHead = dataa[0];
		console.log("Got Account Head");
		Api.restApiCall("core.contacts","::find",getOrg,function(data){
			//console.log(data);
			if(data){
				org = data[0];
				console.log("Organization : "+org.displayName);
				if(org && org.displayName)
				{	
					Api.createTenantObject(acntHead,org,function(obj){
						//console.log(obj);
						Api.restApiCall("core.contacts","::createChannelTenant",obj,function(data1){
							if(data1.success || (data1.messages[0] && data1.messages[0].message && data1.messages[0].message.text == "The tenant already exists."))
							{
								console.log("*** "+org.displayName+" Tenant Created ***");
								console.log("*** Creating Channel Members ***");
								console.log(tenantObj["members"]);
								async.each(tenantObj["members"],function(item){
								//tenantObj["members"].forEach(function(item) { 
									console.log(item);
									if(usernames.indexOf(item["email"])==-1){
									var memData = {
										"firstName":item["name"].split(' ')[0],
										"lastName":item["name"].replace(item["name"].split(' ')[0]+' ',''),
										"username":item["email"]
									};
									Api.checkPerson(memData.username,function(response){
										if(response.success){
											//var contact = response.contact;
											console.log("In Contact Adding");
											var memObj = {
															"company" : org,
															"contact" : response.contact,
															"eCollection_role" :acntHead.roles,
															"password":"passwordone",
															"role" : {
																			"displayName":"Channel Partners",
																			"name":"channelPartnerTeams",
																			"type":"core.role"
																		},
															"type" : "core.create.member.contact"
														};
											Api.restApiCall("core.contacts","::createUserFromContact",memObj,function(added){
												fs.appendFile('ContactsAdded.txt',memData.username+",", function (err) {});	
											});
										}
										else{
											Api.createChannelMemberObject(true,memData,acntHead,function(obj1){
												Api.restApiCall("core.contacts","::addMember",obj1,function(data2){
													if(data2){
													console.log(memData);
													if(data2 && data2["data"]){
													var temp = JSON.stringify(data2["data"]["unknown model"]);
													var unknown = JSON.parse(temp);
													console.log(unknown);
													console.log(unknown[0]["data"]["core.contact/person"][0]["_id"]);
													var id = unknown[0]["data"]["core.contact/person"][0]["_id"];
													var relationships = unknown[0]["data"]["core.contact/person"][0]["relationships"];
													console.log(JSON.stringify(unknown[0]["data"]["core.contact/person"][0]));
													console.log(relationships);
													if(relationships.length<1)
													{
														relationships = [{
																            "type": "core.relationship",
																            "_id": "5284c34608df947a12001fe1",
																            "relation": {
																              "name": "company",
																              "displayName": "Organization",
																              "key": "10005129b1a3a800000006ff",
																              "type": "core.lookup"
																            },
																            "target": {
																              "type": "core.contact/organization",
																              "displayName": "Test Company",
																              "relationships": {
																                
																              },
																              "key": "527b6aa125ef289e170f74f2",
																              "revisionId": 9
																            },
																            "relationships": [
																              
																            ]
																          }];
													}
													relationships[0].target.key = org._id;
													relationships[0].target.displayName = org.displayName;
													var updateData = {
														"relationships":relationships
													};
													console.log("updateData : ")
													console.log(updateData);
													Api.restApiCall("core.contacts","/"+id,updateData,function(data3){
														console.log("Updating Contact");
														console.log(data3);
														if(data3!="")
														{
															console.log('ContactsAdded.txt',memData.username)
															fs.appendFile('ContactsAdded.txt',memData.username+","+tenantObj["tenant"]+",reseller\r\n", function (err) {});	
															fs.appendFile('log.txt',"\n\rUser Created : "+memData.username, function (err) {});
															//console.log(memData.username+" has been Created...");
															//usersDone += memData.username +",";
														}
													});
												}else{
													if(data2 && data2.messages)
													{
														console.log(data2.messages[0].message);
														console.log("Hence Show is ended");
														process.exit(1);
													}
												}
													}
												});
											});
									}

								});
							}else{
								console.log("*** '"+org.displayName+"' "+data1.messages[0].message.text+" ***");
							}
						});
					});
				}else{
					fs.appendFile('organizationSkip.txt',"\r\n"+tenantObj["tenant"], function (err) {});
					console.log(tenantObj["tenant"]+".....Organization Missing.....So this is the show stopper...");
				}
			}
			if (data===undefined)
			{
				fs.appendFile('organizationSkip.txt',"\r\n"+tenantObj["tenant"], function (err) {});
				console.log(".....Organization Missing.....So this is the show stopper...");
				fs.appendFile('log.txt',"\r\nOrganization Escaped : "+tenantObj["tenant"], function (err) {});
			}
		});		
		}


	});
}else{
fs.appendFile('organizationSkip.txt',"\r\n"+tenantObj["tenant"], function (err) {});
}
callback();
}