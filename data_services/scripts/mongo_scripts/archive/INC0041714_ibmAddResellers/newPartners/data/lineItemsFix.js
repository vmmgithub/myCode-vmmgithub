var spawn = require('child_process').spawn;

var mgrUtil = require('./mgrUtil');

var opts = mgrUtil.getOpt(process.argv);

var dbhost = opts.dbhost;
var dbport = opts.dbport;

if(!dbhost || !dbport) {
    console.log('usage: ./RUNME.sh apihost=API_HOST_IP apiport=API_PORT_NUMBER apissl=1 dbhost=MONGO_HOST_IP dbport=MONGO_PORT_NUMBER');
    process.exit(1);
}

var mongo = spawn('mongo', [ dbhost + ':' + dbport + '/testdata', '_lineItemsFix.js' ], {
    env: process.env
});

mongo.stdout.on('data', function(data) {
    console.log(data.toString());
});
mongo.stderr.on('data', function(data) {
    console.log(data.toString());
});
mongo.on('close', function(code) {
   if(code !== 0) {
       console.log('FAILURE: _lineItemsFix returned not-null code');
   }
});

