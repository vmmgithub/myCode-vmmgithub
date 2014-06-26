var fs = require('fs');
var spawn = require('child_process').spawn;


var Util = function() {};

module.exports = Util;

var configPath = './mgrConfig.json';

Util.spawn = function(opt, cb) {
    var cmd = opt.cmd;
    var args = opt.args;

    var stdout = '';
    var stderr = '';
    var proc = spawn(cmd, args);

    proc.stdout.on('data', function(data) {
       stdout += data;
    });

    proc.stderr.on('data', function(data) {
        stderr += data;
    });

    proc.on('close', function (code) {
        cb(null, {
            code: code,
            stdout: stdout,
            stderr: stderr
        });
    });
}

Util.getConfig = function(path) {
    if(!path) path = configPath;

    var configJson = fs.readFileSync(path, 'utf8');

    var config;
    try {
        config = JSON.parse(configJson);
    } catch(e) {
        throw new Error('failed to parse config file at ' + path);
    }

    return config;
}

Util.parseUri = function(uri) {
    var m = uri.match(/^(http|https):([\w\d\.]+):(.+?)\/\/(.+):(\d+)$/);
    if(!m) {
        throw new Error('failed to parse uri ' + uri);
    }

    var r = {};

    r.proto = m[1];
    r.user = m[2];
    r.pass = m[3];
    r.host = m[4];
    r.port = m[5];

    return r;
}

Util.indexOf = function(arr, map) {
    var keys = Object.keys(map);

    var idx = -1;
    for(var i = 0; i < arr.length; i ++) {
        var obj = arr[i];

        var found = true;
        for(var j = 0; j < keys.length; j ++) {
            var key = keys[j];
            if(obj[key] !== map[key]) {
                found = false;
                break;
            }
        }
        if(found) {
            idx = i;
            break;
        }
    }

    return idx;
}

Util.getOpt = function(argv) {
    var opt = {};
    var args = [];
    for(var i = 2; i < argv.length; i ++) {
        var v = argv[i];
        var m = v.match(/^([\w\d]+)=(.*)$/);
        if(m) {
            opt[m[1]] = m[2];
        } else {
            args.push(v);
        }
    }

    opt.ARGS = args;
    return opt;
}

/**
 * usage: parseArgs(process.argv, 'abo:(output)')
 */
Util.parseArgs = function(argv, format) {
    var mod_getopt = require('posix-getopt');
    var parser, option;

    parser = new mod_getopt.BasicParser(format, argv);

    var opts = {};
    while ((option = parser.getopt()) !== undefined) {
        var val = option.optarg;
        if(val === undefined) val = true;
        opts[option.option] = val;
    }

    var args = [];
    var idx = parser.optind();

    while(idx < argv.length) {
        args.push(argv[idx]);
        idx ++;
    }
    opts.ARGS = args;

    return opts;
}