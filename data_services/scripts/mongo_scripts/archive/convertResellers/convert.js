var fs = require('fs');

var rUtil = require('../../tools/release-management/util');

var byOrg = {};
[ 'emea', 'nala' ].forEach(function(tenant) {
    var users = rUtil.readCsvFile({
        file: tenant + '.csv',
        removeQuotes: true
    });

    users.forEach(function(line) {
        var r = {};
        var username = r.username = line[1];
        var name = r.name = line[2];
        var userType = r.userType = line[3];
        var org = r.org = line[4];

        org = org.replace(/^R\) /, '');
        org = org.replace(/^D\) /, '');
        org = org.replace(/^C\) /, '');

        r.org = org;

        if(!byOrg[org]) {
            byOrg[org] = {
                tenantName: tenant,
                resellerName: org,
                members: []
            }
        }

        var profile;
        if(userType == 'Reseller') profile = 'resellerRep';
        else if(userType == 'Disti') profile = 'distributorRep';

        if(!profile) {
            return;
        }

        var members = byOrg[org].members;

        var tmp = members.filter(function(v) { return v.username == username });
        if(tmp.length > 0) {
            console.log('user ' + username + 'already exists, skip');
        } else {
            members.push({
                tenant: tenant,
                name: name,
                username: username,
                password: 'passwordone',
                profile: profile
            })
        }

    });
});

var idx = 1;
for(var orgName in byOrg) {
    var org = byOrg[orgName];

    fs.writeFileSync('configs/' + org.tenantName + "-" + idx + ".json", JSON.stringify(org, null, 2));
    idx ++;
}

//{
//    "name": "Adele Singh",
//    "username" : "adele.singh@avnet1.com",
//    "password" : "passwordone",
//    "tenant" : "avnet1",
//    "profile" : "channelPartnerAdmin"
//},