var mgrUtil = require('./mgrUtil');

var opts = mgrUtil.getOpt(process.argv);

var apiHost = opts.apihost;
var apiPort = opts.apiport;
var dbhost = opts.dbhost;
var dbport = opts.dbport;

if(!apiHost || !apiPort || !dbhost || !dbport) {
    console.log('usage: ./RUNME.sh apihost=API_HOST_IP apiport=API_PORT_NUMBER apissl=1 dbhost=MONGO_HOST_IP dbport=MONGO_PORT_NUMBER');
    process.exit(1);
}
