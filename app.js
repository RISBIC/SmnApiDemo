var express = require('express'),
    anyDB   = require('any-db'),
    moment  = require('moment'),
    _       = require('underscore');

// Config
var smn = {
    ip_address: process.env.OPENSHIFT_NODEJS_IP || '0.0.0.0',
    port: process.env.OPENSHIFT_NODEJS_PORT || 8080,
    dbURL: 'postgres://admin3s86tsw:adlkvaj2VLU7@52d5139a5973ca520100008a-testarjuna.rhcloud.com:58216/smn'
};

// create app
var app = express();

// enable gzip compression
app.use(express.compress());

// create DB connection pool
var pool = anyDB.createPool(smn.dbURL, {min: 2, max: 10});

app.listen(smn.port, smn.ip_address, function () {
    console.log('[%s] Node server started on %s:%d, (%s minutes from UTC) ...', moment.utc().format(), smn.ip_address, smn.port, moment().zone());

    // Modules to load (and the order to load them in)
    var modules = [ 'parameters', 'cors', 'outstations', 'reports', 'feed' ];

    // Load each module
    _.each(modules, function (moduleName) {
        loadModule(moduleName, app, pool);
    });
});

// Load a module and pass it an instance of the app and DB pool
function loadModule (moduleName, app, pool) {
    var fileName = __dirname + '/modules/' + moduleName;

    console.log('Loading module: [%s]', fileName);
    require(fileName)(app, pool);
}