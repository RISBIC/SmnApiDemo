/**
 * 'app' is an instance of an ExpressJS web application
 * 'pool' is an instance of an AnyDB DB connection pool
 *
 * Both instances are passed in from server.js
 */
module.exports = exports = function (app, pool) {

    /**
     * Add CORS headers to all HTTP request types
     */
    app.all('*', function(req, res, next) {
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Headers", "X-Requested-With");

        next();
    });
};