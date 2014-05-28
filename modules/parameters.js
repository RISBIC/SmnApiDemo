/**
 * Dependencies
 */
var validator = require('validator');

/**
 * 'app' is an instance of an ExpressJS web application
 * 'pool' is an instance of an AnyDB DB connection pool
 *
 * Both instances are passed in from server.js
 */
module.exports = exports = function (app, pool) {

    /**
     * Define and validate the :id parameter for route mapping
     *
     * NOTE: The signature (req, res, next, value) or (req, res, next)
     * is a standard Node.js "middleware" function signature
     */
    app.param('id', function (req, res, next, value) {
        if (validator.isInt(value)) {
            req.params.id = validator.toInt(value);
            next();
        } else {
            next(new Error(':id URL parameter must be an integer'));
        }
    });

    /**
     * Define and validate the :reporttype parameter for route mapping
     */
    app.param('reporttype', function (req, res, next, value) {
        var values = [ 'speed', 'volume' ];

        if (values.indexOf(value) !== -1) {
            req.params.reporttype = value;
            next();
        }
        else {
            next(new Error(':reporttype URI parameter contained an invalid value (got: "' + value + '" expected: <' + values.join('|') + '>)'));
        }
    });

    /**
     * Define and validate the :period parameter for route mapping
     */
    app.param('period', function (req, res, next, value) {
        var values = [ '15min', 'hour', 'day', 'week' ];

        if (values.indexOf(value) !== -1) {
            req.params.period = value;
            next();
        }
        else {
            next(new Error(':period URI parameter contained an invalid value (got: "' + value + '" expected: <' + values.join('|') + '>)'));
        }
    });

};