var _         = require('underscore'),
    moment    = require('moment'),
    squel     = require('squel'),
    validator = require('validator');

// app and pool are passed FROM app.js
module.exports = exports = function (app, pool) {

        // map GET /stuart -> inline function
        app.get('/stuart', function (req, res) {

                var query = squel.select()
                        .field('dbdata.outstationid')
                        .field('MAX(dbdata.inserttime) AS lasttime')
                        .from('dbdata')
                        .group('dbdata.outstationid')
                        .order('dbdata.outstationid', true);

                debug_request(req, query);

                var start = new Date().getTime();
                pool.query(query.toString(), function (err, result) {
                        if (err) {
                                error_json(res, 500, 'error running query', err);
                                return console.error(err);
                        }

                        res.jsonp(_.map(result.rows, latestdata_json));

                        var end = new Date().getTime();

                        debug_querytime(req, end - start);
                });
        });

        // map GET /feed -> inline function
        app.get('/feed', function (req, res) {

                var query = squel.select()
                        .from('dbdata')
                        .field('classcode')
                        .field('direction')
                        .field('lane')
                        .field('outstationid')
                        .field('site')
                        .field('speed')
                        .field('time')
                        .field('inserttime')
                        .order('inserttime', false)
                        .limit(10000);

                if (req.query.outstationid)
                {
                        var outstationid = validator.toInt(req.query.outstationid);

                        query = query.where('outstationid = ?', outstationid);
                }

                if (req.query.from)
                {
                        var from = validator.toInt(req.query.from);

                        query = query.where('inserttime > ?', from);
                }

                debug_request(req, query);

                var start = new Date().getTime();
                pool.query(query.toString(), function (err, result) {
                        if (err) {
                                error_json(res, 500, 'error running query', err);
                                return console.error(err);
                        }

                        res.jsonp(feedreport_json(result.rows));

                        var end = new Date().getTime();

                        debug_querytime(req, end - start);
                });
        });
};

function latestdata_json(row)
{
        var result = {};

        result.outstationid   = row.outstationid;
        result.lastinserttime = (new Date(parseInt(row.lasttime))).toISOString();

        return result;
}

function feedreport_json(rows) {
        var result = {};

        if (rows[0])
        {
                 result.lastinserttime = parseInt(rows[0].inserttime);
                 result.vehiclerecords = _.map(rows, vehiclerecord_json);
        }
        else
                 result.lastinserttime = 0;

        return { 'feedReport': result };
}

function vehiclerecord_json(row) {
        var result = {};

        result.outstationid = row.outstationid;
        result.time         = parseInt(row.time);
        result.speed        = row.speed;
        result.site         = row.site;
        result.lane         = row.lane;
        result.direction    = row.direction;
        result.classcode    = row.classcode;

        return result;
}

function debug_request(req, query)
{
        console.log('[%s] URI: [%s], SQL: [%s]', moment.utc().format(), req.url, query.toString());
}

function debug_querytime(req, time)
{
        console.log('[%s] URI: [%s], Query time: [%s] ms', moment.utc().format(), req.url, time);
}

function error_json(res, statusCode, message, err) {
        // ISE statusCode if not provided
        statusCode = statusCode || 500;

        // Generic message if not provided
        message = message || 'An unspecified error occurred while handling the request.';

        response = {
                statusCode: statusCode,
                message: message
        };

        if (err) {
                response.cause = err;
        }

        res.jsonp(statusCode, response);
}
