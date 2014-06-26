var cookie=null;
var fs = require("fs");
var request = require('request');
var Api = function(opt) {
	this.serverName = opt.serverName;
	this.tenant = opt.tenant;
	this.userName = opt.userName;
}
Api.prototype.login = function(callback){
		var userName = this.userName;
		var authentication = '{"username":"'+this.userName+'@'+this.tenant+'.com","password":"passwordone"}';
		var login = request.post({
		key:    fs.readFileSync('config-t2.key'),
		cert:   fs.readFileSync('config-t2.crt'),
		requestCert:        true,
		rejectUnauthorized: false,
		  headers: {
					'Content-Length': Buffer.byteLength(authentication),
					'content-type': 'application/json; charset=UTF-8',
					},
					port: 443,
		  url:     'https://'+this.serverName+'/login.json',
		  body:    authentication
		}, function(error, response, body){
			if(response && response.statusCode ==200){
				console.log("Success : Logged in as "+userName);
				cookie = response.headers["set-cookie"][0].split(';')[0];
				callback("Success");
			}else{
				console.log("***Error While Login***");
				exit();
			}
		});
	};
Api.prototype.restApiCall = function (entity,method,postData,callback) {
	// var postData = {"filter":{"displayName":"Bill Moor"}};
	var login = request.post({
		key:    fs.readFileSync('config-t2.key'),
		cert:   fs.readFileSync('config-t2.crt'),
		requestCert:        true,
		rejectUnauthorized: false,
		  headers: {
					'content-type': 'application/json; charset=UTF-8',
					'Cookie':cookie,
					'Connection':'keep-alive',
					'Referer': 'https://'+this.serverName+'/tests/test.html',
					'content-type': 'application/json; charset=UTF-8',
					'tenant': this.tenant
					},
		  port: 443,
		  url:     'https://'+this.serverName+'/rest/api/'+this.tenant+'/'+entity+method
		}, function(error, response, body){
			if(response && response.statusCode ==200){
				//console.log("response body");
				//console.log("getting this : "+body);
				try{
					return callback(JSON.parse(body));
				}catch(e){
					callback();
				}
			}else{
				console.log("***Error While Processing the Request***");
				exit();
			}
		});
	login.write(JSON.stringify(postData));
};
Api.prototype.createTenantObject = function(accountHead,Org,callback){
	var createTenantData = {
		"adminFirstName":"admin",
		"adminLastName":Org.displayName.toLowerCase().split(' ').join(''),
		"adminPassword":"welcome",
		"adminUser":"admin@"+Org.displayName.toLowerCase().split(' ').join('')+".com",
		"currency":{
			"displayName":"USD",
			"name":"usd"
		},
		"eCollection_parentRole":accountHead.roles,
		"locale":{
			"displayName":"English (United States)",
			"name":"en-us"
		},
		"organization":{
			"_id":Org._id,
			"displayName":Org.displayName,
			"type":"core.contact/organization"
		},
		"parentAdminRole":{
			"name":"channelPartnerAdmin"
		},
		"parentRole":{
			"displayName":"Channel Partner Admin",
			"name":"channelPartnerAdmin",
			"type":"core.role",
			"uiProfile":"channelPartnerAdmin"
		},
		"parentTeam":accountHead,
		"template":{
			"name":"cpo-template"
		},
		"tenant":Org.displayName,
		"tenantName":Org.displayName.toLowerCase().split(' ').join(''),
		"type":"create.channel.tenant.input"
	};
	callback(createTenantData);
}
Api.prototype.createChannelMemberObject = function(reseller,memData,accountHead,callback){
	var displayName,name;
	if(reseller){
		displayName = "Channel Partners";
		name = "channelPartnerTeams";
	}else{
		//distributor settings
		displayName = "Distributor Patners";
		name = "distributorTeams";
	}
	var channelMember = {
		"eCollection_parentRole":accountHead.roles,
		"firstName":memData.firstName,
		"lastName":memData.lastName,
		"password":"passwordone",
		"role":{
			"displayName":displayName,
			"name":name,
			"type":"core.role"
		},
		"team":accountHead,
		"type":"core.add.member.input",
		"username":memData.username
	};
	callback(channelMember);
}

function exit(){
	process.exit(1);
}

module.exports = Api;
