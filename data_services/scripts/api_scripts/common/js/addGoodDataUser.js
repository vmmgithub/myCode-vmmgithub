#!/usr/bin/env node

var _ = require("underscore"),
	async = require("async"),
	request = require('request'),
	csvHelperInstance = require("../../lib/helpers/CsvHelper"),
	h = require('../../common/js/helper');

var input = require('optimist')
    .usage('\nREADME: This is a utility to add users to GoodData projects using an input file\
        \n FirstName, LastName, Email, Tenant \
        \n\nUsage: $0')
    .alias('h', 'host').describe('h', 'Specify host').default('h', 'secure.gooddata.com')
    .alias('n', 'port').describe('n', 'Specify port').default('n', '443')
    .alias('u', 'user').describe('u', 'Specify user').default('u', 'gooddata@servicesource.com')
    .alias('m', 'manager').describe('m', 'Specify manager').default('m', 'GD_Manager@servicesource.com')
    .alias('p', 'password').describe('p', 'Specify password').default('p', 'passwordone')
    .alias('d', 'domain').describe('d', 'SSO Domain').default('d', 'servicesource')
    .alias('f', 'file').describe('f', 'File Name')
    .alias('r', 'role').describe('r', 'Role Name').default('r', 'Embedded Dashboard Only')
    .alias('l', 'limit').describe('l', 'Concurrent threads').default('l', 5)
    .alias('o', 'operation').describe('o', 'Operations available [logProjects|logProjectUsers|logUsers|addUser]')
    .demand(['o'])
    .argv;

input.host = ((input.port == '443') ? 'https' : 'http') + '://' + input.host;

var superJar = request.jar();
var managerJar = request.jar();
var cache = {
	projects: {}, 
	users: {},
	profileId: null
};

var authenticateForAPI = function(admin, callback) {
	var loginPayload = {
		url: input.host + '/gdc/account/login',
		json: true,
		jar: (admin ? superJar : managerJar),
		body: {
			postUserLogin : {
				login: (admin ? input.user : input.manager),
		        password: input.password, 
				remember : 1
			}
		}
	},
	tokenPayload = {
		url: input.host + '/gdc/account/token',
		jar: (admin ? superJar : managerJar),
		headers: {'Accept': 'application/yaml'},
		json: true
	};

	request.post(loginPayload, function (err, response, body) {
		if (err || response.statusCode != 200 || !body)
			return callback('Error logging to GoodData ' + JSON.stringify(err || body));

		if (!body.userLogin || !body.userLogin.profile) return callback('No userLogin data');
		var profileId = body.userLogin.profile.split('/')[4];
		if (admin) cache.profileId = profileId;

		request.get(tokenPayload, function(err, response, body) {
			if (err || response.statusCode != 200 || !body)
				return callback('Error getting token ' + JSON.stringify(err || body));

			callback(err);
		});
	});
};

var executeGoodDataAPI = function(url, method, payload, admin, callback) {
	var message = {
		url: input.host + url,
		jar: (admin ? superJar : managerJar),
		method: method,
		headers: {
			'Accept': 'application/json', 
			'Content-Type': 'application/json'
		},
		json: true		
	};

	if (payload) message.body = JSON.stringify(payload);

	request(message, function(err, response, body) {
		if (err || !response || !body || body.error)
			return callback('Error invoking GoodData API ' + JSON.stringify(err || body), body);

		callback(err, body);
	});
};

var getEmbeddedRole = function(projectId, callback) {
	executeGoodDataAPI(projectId + '/roles', 'GET', null, true, function(err, data) {
		if (err || !data || !data.projectRoles || !data.projectRoles.roles) return callback(err || 'No roles');

		_.each(data.projectRoles.roles, function(role) {
			executeGoodDataAPI(role, 'GET', null, true, function(err, d) {
				if (err || !d || !d.projectRole) return callback(err || 'No project roles');

				if (d.projectRole.meta.title == input.role)
					callback(err, role);
			});
		});
	});		
};

var init = function(callback) {
	async.series([
		function(cb) {
			authenticateForAPI(true, cb);	
		},
		function(cb) {
			authenticateForAPI(false, cb);	
		},
		function(cb) {
			// get user information to validate who exists
			executeGoodDataAPI('/gdc/account/domains/' + input.domain + '/users', 'GET', null, true, function(err, data) {
				if (err || !data || !data.accountSettings || !data.accountSettings.items) return cb(err || 'No users defined');

				_.each(data.accountSettings.items, function(item) {
					cache.users[item.accountSetting.email] = item.accountSetting.links.self;
				});

				cb(err);
			});
		},
		function(cb) {
			executeGoodDataAPI('/gdc/account/profile/' + cache.profileId + '/projects', 'GET', null, true, function(err, data) {
				if (err || !data || !data.projects) return cb(err || 'No projects defined');

				// return only after reading everything about all projects and the users
				var done = _.after(data.projects.length, cb);

				// get project details including embedded role that needs a couple lookups
				_.each(data.projects, function(p) {
					getEmbeddedRole(p.project.links.self, function(err, role) {
						var pName = p.project.meta.title.toLowerCase();
						cache.projects[pName] = {
							title: p.project.meta.title,
							projectId: p.project.links.self,
							roleId: role
						};

						return done(err);
					});
				});
			});	
		},
		function(cb) {
			var done = _.after(_.keys(cache.projects).length, cb);
			_.each(_.keys(cache.projects), function(p) {
				var project = cache.projects[p];
				executeGoodDataAPI(project.projectId + '/users', 'GET', null, true, function(err, data) {
					if (err || !data || !data.users) return done(err || 'No users defined');

					project.users = [];
					_.each(data.users, function(u) {
						project.users.push(u.user.content.email);
					});
					done();
				});		
			});
		},		
	], callback);
};

var mangleEmail = function(email) {
	var domain = email.split('@')[1];
	var base = email.split('@')[0];
	var mangledemail = email;

	if (!h.contains(domain.toLowerCase(), input.domain)) 
		mangledemail = base + '+' + domain + '@' + input.domain + '.com';

	return mangledemail;
};

var createUser = function(firstName, lastName, email, callback) {
	var mangledemail = mangleEmail(email);
	if (cache.users[mangledemail]) return callback(null, cache.users[mangledemail]);

	var ssopwd = 'passwordone';
	var userCreatePayload = {
		accountSetting: {
			login: mangledemail,
			password: ssopwd,
			verifyPassword: ssopwd,
			email: mangledemail,
			firstName: firstName,
			lastName: lastName,
			ssoProvider: input.domain + '.com'
		}
	};

	executeGoodDataAPI('/gdc/account/domains/' + input.domain + '/users', 'POST', userCreatePayload, true, function(err, data) {
		callback(err, data && data.uri);
	});	
};

var processRecord = function(firstName, lastName, email, tenant, callback) {
	var project;

	_.each(_.keys(cache.projects), function(pName) {
		if (h.contains(pName, tenant.toLowerCase())) project = cache.projects[pName];
	});

	if (!project) return callback('No project found for ' + tenant);

    createUser(firstName, lastName, email, function(err, userId) {
    	if (err) return callback(err || 'Unable to create user');

		var payload = {
			user: {
				content: {
					status: 'ENABLED',
	        		userRoles: [project.roleId]},
	        		links: {self: userId}
	    		}
	    	};
		executeGoodDataAPI(project.projectId + '/users', 'POST', payload, true, function(err, data) {
			callback(err, data && data.projectUsersUpdateResult);
		});
    });

};

h.log('debug', 'Started bootstrapping ... ');
init(function(err) {
	h.log('debug', 'Finished bootstrapping ... ');

	if (input.operation == 'logProjects') {
		_.each(_.keys(cache.projects), function(p) {
			h.log('info', (cache.projects[p].projectId + '::' + cache.projects[p].title));
		});
	}

	if (input.operation == 'logUsers') {
		_.each(_.keys(cache.users), function(u) {
			h.log('info', (u + '::' + cache.users[u]));
		});
	}

	if (input.operation == 'logProjectUsers') {
		_.each(_.keys(cache.projects), function(p) {
			var project = cache.projects[p];
			h.log('info', (project.projectId + '::' + project.title));
			_.each(project.users, function(user) {
				console.log(project.projectId + '|' + project.title + '|' + user);
			});
		});
	}

	if (input.operation == 'addUser') {

		if (_.isEmpty(input.file)) return h.log('error', 'Input file needed ');

		var csvHelper = new csvHelperInstance();
		csvHelper.readAsObj(input.file, function (data) {
		    async.eachLimit(data, input.limit, function (csvRecord, callback) {
		    	var firstName = csvRecord["FirstName"], 
		    		lastName = csvRecord["LastName"], 
		    		email = csvRecord["Email"], 
		    		tenant = csvRecord["Tenant"];

		    	if (!firstName || !lastName || !email || !tenant) return callback('Insufficient data for ' + csvRecord);

				processRecord(firstName, lastName, email, tenant, function(err, data) {
					if (err) h.log('error', 'FAIL: ' + email + ' on ' + tenant + '|' + err );
					else h.log('info', 'SUCCESS: ' + email + ' on ' + tenant);

					callback();
				});
			}, function (err) {
	            h.log('info', 'DONE ' + err);
	        });
		});
	} 

});
