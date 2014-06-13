/**
 * Dependencies
 */
var _         = require('underscore'),
    moment    = require('moment'),
    validator = require('validator'),
    squel     = require('squel'),
    mongojs   = require('mongojs');


/**
 * 'app' is an instance of an ExpressJS web application
 * 'pool' is an instance of an AnyDB DB connection pool
 *
 * Both instances are passed in from server.js
 */
module.exports = exports = function (app, pool) {

    var cache = mongojs('mongodb://admin:ReICPU9Bk54K@127.9.230.130:27017/reports', ['reports']);

    // View available report types for outstations
    app.get('/outstations/:id/:reporttype', function (req, res) {
        var outstationid = req.params.id;
        var reporttype = req.params.reporttype;
        var periods = [ '15min', 'hour', 'day', 'week' ];
        var response = {};

        periods.forEach(function (period) {
            response[period] = '/outstations/' + outstationid + '/' + reporttype + '/' + period;
        });

        res.jsonp(response);
    });

    // Generate a speed/volume report for an outstation for the specified period
    app.get('/outstations/:id/:reporttype/:period', function (req, res) {
        var queryKey = {
            outstation: req.params.id,
            reportType: req.params.reporttype,
            reportPeriod: req.params.period
        };

        if (req.query.lanes) {
            queryKey.lanes = (req.query.lanes === 'true');
        }

        if (req.query.count && validator.isInt(req.query.count)) {
            queryKey.count = validator.toInt(req.query.count);
        }

        if (req.query.timestart && validator.isInt(req.query.timestart)) {
            queryKey.timestart = validator.toInt(req.query.timestart);
        }

        if (req.query.timestop && validator.isInt(req.query.timestop)) {
            queryKey.timestop = validator.toInt(req.query.timestop);
        }

        if (req.query.order) {
            queryKey.order = (req.query.order.toLowerCase() === 'desc') ? 'desc' : 'asc';
        }

        if (isCacheable(queryKey)) {
            cacheLookup(req, res, queryKey);
        } else {
            databaseLookup(req, res, queryKey, false);
        }
    });

    // Try and hit the cache (MongoDB) first
    function cacheLookup(req, res, queryKey) {
        // hit MongoDB
        cache.reports.findOne({queryKey: queryKey}, function (err, result) {
            if (err) {
                sendError(res, 500, 'error querying cache', err);
                return console.error(err);
            }

            if (result) {
                res.jsonp(result.report);
            } else {
                databaseLookup(req, res, queryKey, true);
            }
        });
    }

    // Compute from the database
    function databaseLookup(req, res, queryKey, updateCache) {
        var outstationid = queryKey.outstation;
        var reporttype = queryKey.reportType;
        var period = queryKey.reportPeriod;

        var query = squel.select();

        if (req.query.lanes && req.query.lanes == 'true')
        {
            query = query.from(reporttype + '_summary_lane(' + periodLength(period) + ', ' + outstationid + ')');
        }
        else
        {
            query = query.from(reporttype + '_summary(' + periodLength(period) + ', ' + outstationid + ')');
        }

        if (req.query.count) {
            var count = validator.toInt(req.query.count);

            query = query.limit(count);
        }

        if (req.query.timestart)
        {
            var timestart = validator.toInt(req.query.timestart);

            query = query.where('period >= ?', timestart);
        }

        if (req.query.timestop)
        {
            var timestop = validator.toInt(req.query.timestop);

            query = query.where('period < ?', timestop);
        }

        var order = 'asc';
        if (req.query.order)
        {
            order = (req.query.order.toLowerCase() === 'desc') ? 'desc' : 'asc';
        }

        query = query.order('period', (order != 'desc'));

        if (req.query.lanes && req.query.lanes == 'true')
        {
            query = query.order('lane', true);
        }

        debugRequest(req, query);
        pool.query(query.toString(), function (err, result) {
            if (err) {
                sendError(res, 500, 'error running query', err);
                return console.error(err);
            }

            /*
             * FILL IN GAPS IN DATA
             */
            var gap = periodLength(period);

            var response = result.rows;

            // Create rows to fill in the gaps where 0 vehicles were seen for a given summary period
            // NOTE: only try and fill in for >= 15 min summaries.
            if (gap >= periodLength('15min'))
            {
                var min_period = parseInt(_.min(result.rows, function (row) { return parseInt(row.period); }).period);
                var max_period = parseInt(_.max(result.rows, function (row) { return parseInt(row.period); }).period);

                // Calculate lower bound of search
                if (req.query.timestart)
                {
                    var timestart = validator.toInt(req.query.timestart);
                    timestart = timestart - (timestart % gap);

                    min_period = (timestart < min_period) ? timestart : min_period;
                }

                // Calculate upper bound of search
                if (req.query.timestop)
                {
                    var timestop = validator.toInt(req.query.timestop);
                    timestop = timestop - (timestop % gap);

                    max_period = (timestop > max_period) ? timestop : max_period;
                }

                var missing = [];

                // Calculate where the missing rows are
                for (var i = min_period; i < max_period; i += gap)
                {
                    if (_.where(result.rows, {period: i.toString()}).length === 0)
                    {
                        missing.push(generateEmptyReportJSON(reporttype, outstationid, i));
                    }
                }

                response = _.union(result.rows, missing);

                if (order == 'desc')
                {
                    // period DESC, lane ASC
                    response.sort(function (l, r) { return parseInt(r.period) - parseInt(l.period)  ||  parseInt(l.lane) - parseInt(r.lane); });
                }
                else
                {
                    // period ASC, lane ASC
                    response.sort(function (l, r) { return parseInt(l.period) - parseInt(r.period)  ||  parseInt(l.lane) - parseInt(r.lane); });
                }
            }

            if (updateCache) {
                cache.reports.save({queryKey: queryKey, report: response });
            }

            res.jsonp(response);
        });
    }

    /**
     * UTILITY FUNCTIONS
     */
    // Can this query be cached (i.e., does it fall into a period still being collected for)
    function isCacheable (queryKey) {
        var now = moment.utc().valueOf();
        now = (now - (now % periodLength(queryKey.reportPeriod)));

        if ((queryKey.timestart && (queryKey.timestart <= now)) && (queryKey.timestop && (queryKey.timestop <= now))) {
            return true;
        }

        return false;
    }

    // Parameter name to period length (in milliseconds)
    function periodLength (period) {
        switch (period)
        {
            case '15min':
                return 900000;
            case 'hour':
                return 3600000;
            case 'day':
                return 86400000;
            case 'week':
                return 604800000;
        }
    }

    // Create empty/synthetic report rows to fill in time gaps
    function generateEmptyReportJSON(type, id, timestamp)
    {
        if (type == 'speed')
        {
            return {
                "outstationid": id,
                "period": timestamp.toString(),
                "lane": 0,
                "volume": 0,
                "speed_min": 0,
                "speed_max": 0,
                "speed_avg": 0,
                "speed_stddev": 0,
                "speed_85th_percentile": 0,
                "_0_to_10": 0,
                "_11_to_20": 0,
                "_21_to_30": 0,
                "_31_to_40": 0,
                "_41_to_50": 0,
                "_51_to_60": 0,
                "_61_to_70": 0,
                "_70_plus": 0,
                "!SYNTHETIC": true
            };
        }
        else if (type == 'volume')
        {
            return {
                "outstationid": id,
                "period": timestamp.toString(),
                "lane": 0,
                "volume": 0,
                "_unclassified": 0,
                "_0_to_52": 0,
                "_52_to_70": 0,
                "_70_to_110": 0,
                "_110_plus": 0,
                "!SYNTHETIC": true
            };
        }
        else
        {
            return {};
        }
    }

    function debugRequest(req, query)
    {
        console.log('[%s] URI: [%s], SQL: [%s]', moment.utc().format(), req.url, query.toString());
    }

    function sendError(res, statusCode, message, err) {
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
};