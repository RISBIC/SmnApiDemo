/**
 * Dependencies
 */
var _         = require('underscore'),
    moment    = require('moment'),
    json2csv  = require('json2csv'),
    validator = require('validator'),
    squel     = require('squel');

/**
 * 'app' is an instance of an ExpressJS web application
 * 'pool' is an instance of an AnyDB DB connection pool
 *
 * Both instances are passed in from server.js
 */
module.exports = exports = function (app, pool, cachePool) {

    /**
     * Redirect / to /outstations
     */
    app.get('/', function (req, res) {
        res.redirect('/outstations');
    });

    /**
     * GET information about all outstations, supports location searching
     */
    app.get('/outstations', function (req, res) {
        var query = squel.select()
            .from('outstations')
            .field('outstationid')
            .field('name')
            .field('description')
            .field('orientation')
            .field('speedlimit')
            .field('ST_AsGeoJson(location,15,2)', 'location')
            .order('outstationid');

        // Location based searchin (using PostGIS)
        if (req.query.latitude && req.query.longitude && req.query.radius) {
            try {
                var latitude = validator.toFloat(req.query.latitude);
                if ((latitude < -90) || (latitude > 90)) {
                    throw new Error('latitude must be between -90 and 90');
                }

                var longitude = validator.toFloat(req.query.longitude);
                if ((longitude < -180) || (longitude > 180)) {
                    throw new Error('longitude must be between -180 and 180');
                }

                var radius = validator.toInt(req.query.radius);
                if (radius < 1) {
                    throw new Error('radius must be >= 1');
                }

                query = query.where("ST_DWithin(location, 'POINT(? ?)', ?)", longitude, latitude, radius);
            }
            catch (err) {
                error_json(res, 400, 'Invalid location parameters', err);
                return console.error('invalid location parameters', err);
            }
        }

        // debug_request(req, query);
        pool.query(query.toString(), function (err, result) {
            if (err) {
                error_json(res, 500, 'error running query', err);
                return console.error(err);
            }

            res.jsonp(_.map(result.rows, outstation_json));
        });
    });


    /**
     * GET an individual outstation (and its latest update)
     */
    app.get('/outstations/:id', function (req, res) {
        var outstationid = validator.toInt(req.params.id);

        var query = squel.select()
            .from('outstations')
            .field('outstations.outstationid')
            .field('outstations.name')
            .field('outstations.description')
            .field('outstations.speedlimit')
            .field('ST_AsGeoJson(outstations.location,15,2)', 'location')
            .field('MAX(dbdata.time)', 'lastupdate')
            .field('outstation_lanes.lane', 'lane')
            .field('outstation_lanes.orientation', 'orientation')
            .where('outstations.outstationid = ?', outstationid)
            .left_join('dbdata', null, 'dbdata.outstationid = outstations.outstationid')
            .left_join('outstation_lanes', null, 'outstation_lanes.outstationid = outstations.outstationid')
            .group('outstations.outstationid')
            .group('outstation_lanes.lane')
            .group('outstation_lanes.orientation');

        // debug_request(req, query);
        pool.query(query.toString(), function (err, result) {
            if (err) {
                error_json(res, 500, 'error running query', err);
                return console.error(err);
            }

            if (result.rows.length === 0) {
                error_json(res, 404, 'No outstation with outstationid = ' + outstationid + ' could be found.');
            }
            else {
                var outstation = outstation_json(result.rows[0]);

                if (result.rows.length > 1) {
                    outstation.lanes = outstation_lane_json(result.rows);
                }

                res.jsonp(outstation);
            }
        });
    });


    /**
     * GET the individually reported records for an individual outstation, ordered by record generation time DESC
     */
    app.get('/outstations/:id/records', function (req, res) {
        var outstationid = validator.toInt(req.params.id);

        var query = squel.select()
            .from('dbdata')
            .field('_id', 'id')
            .field('classcode')
            .field('direction')
            .field('lane')
            .field('outstationid')
            .field('site')
            .field('speed')
            .field('time')
            .where('outstationid = ?', outstationid)
            .order('time', false);

        // Support pagination by record ID
        if (req.query.maxid) {
            var maxid = validator.toInt(req.query.maxid);

            query = query.where('id <= ?', maxid);
        }

        // Page size for pagination
        if (req.query.count)
        {
            if (req.query.count != 'all')
            {
                var count = validator.toInt(req.query.count);
                query = query.limit(count);
            }
        }
        else {
            query = query.limit(1000);
        }

        // Support pagination/filtering by record timestamp
        if (req.query.timestart)
        {
            var timestart = validator.toInt(req.query.timestart);

            query = query.where('time >= ?', timestart);
        }

        if (req.query.timestop)
        {
            var timestop = validator.toInt(req.query.timestop);

            query = query.where('time < ?', timestop);
        }

        // debug_request(req, query);
        pool.query(query.toString(), function (err, result) {
            if (err) {
                error_json(res, 500, 'error running query', err);
                return console.error(err);
            }

            if (req.query.format && req.query.format == 'csv')
            {
                json2csv({data: result.rows, fields: ['id', 'classcode', 'direction', 'lane', 'outstationid', 'site', 'speed', 'time']}, function(err, csv) {
                    if (err) {
                        error_json(res, 500, 'error running query', err);
                        return console.error(err);
                    }

                    res.set('Content-Type', 'text/csv');
                    res.send(csv);
                });
            }
            else
            {
                res.jsonp(result.rows);
            }
        });
    });

    // Build/deliver a JSON object for an error
    function error_json(res, statusCode, message, err) {
        // ISE statusCode if not provided
        statusCode = statusCode || 500;

        // Generic message if not provided
        message = message || 'An unidentified error occurred while handling the request';

        response = {
            statusCode: statusCode,
            message: message
        };

        if (err) {
            response.cause = err;
        }

        res.jsonp(statusCode, response);
    }

    // Convert raw SQL results to to our response
    function outstation_json(row) {
        var result = {};

        result.outstationid = row.outstationid;
        result.name = row.name;
        result.description = row.description;
        result.speedlimit = row.speedlimit;

        result.location = JSON.parse(row.location);

        result.status = '/outstations/' + row.outstationid;
        result.records = '/outstations/' + row.outstationid + '/records';
        result.volume = '/outstations/' + row.outstationid + '/volume';
        result.speed = '/outstations/' + row.outstationid + '/speed';

        if (row.lastupdate) {
            result.lastupdate = parseInt(row.lastupdate);
        }

        return result;
    }

    function outstation_lane_json(rows) {
        var result = [];

        _.each(rows, function (row) {
            result.push({lane: row.lane, orientation: row.orientation});
        });

        return result;
    }

    function debug_request(req, query)
    {
        console.log('[%s] URI: [%s], SQL: [%s]', moment.utc().format(), req.url, query.toString());
    }
};