#!/usr/bin/env node

/**
 * server.js
 *
 * This is the master airbrake-proxy process responsible for controlling the
 * main worker cluster that deals with listening to Airbrake client requests
 * and storing them in Airbrake.
 */

/*
 * Modules
 */

var util = require('util');
var http = require('http');
var cluster = require('cluster');
var microtime = require('microtime');
var nodestatsd = require('node-statsd').StatsD;
var xmlparse = require('xml2js').parseString;
var uuid = require('./lib/uuid');

/*
 * Configuration
 */

// Load configuration from disk
try {
	var configjson = require('./config/config.json');
} catch (e) {
	util.log("Could not load configuration, did you create config.json?");
	process.exit(1);
}

// Parse configuration against defaults
var config = require('./lib/config').config(configjson);

// Choose protocol to connect to Airbrake with
config.airbrake.connection = (config.airbrake.protocol === "https") ? require('https') : require('http');

// Create Redis connection
var redis = require('redis').createClient(config.redis.port, config.redis.host, {'enable_offline_queue': false});

redis.on('error', function (error) {
	util.log("Could not create a Redis client connection to " + config.redis.host + ":" + config.redis.port);
	process.exit(6);
});

// Create StatsD connection
var statsd = new nodestatsd(config.statsd.host, config.statsd.port);

// Airbrake style XML response to send to client
var xmlresponse = '<?xml version="1.0"?><notice><id>{UUID}</id><url>http://' + config.listen.hostname + ':' + config.listen.port + '/locate/{UUID}</url></notice>';

/*
 * Worker cluster
 */

if (cluster.isMaster) {
	// Give the master a proper process title in the process list
	process.title = 'airbrake-proxy';

	// Helper function to fork a child worker
	var fork = function () {
		cluster.fork();
	};

	// Create enough workers to satisfy passed worker count
	for (var child = 0; child < config.listen.workers; child++) {
		fork();
	}

	// If a cluster worker exits, create a new one
	cluster.on('exit', function (worker, code, signal) {
		fork();
	});
} else if (cluster.isWorker) {
	util.log('Child ' + cluster.worker.process.pid + ' started, listening to client requests');

	// Make a request to Airbrake, and store the UUID pairing in Redis
	var store = function (requesturl, responseuuid, data) {
		var airbrakeRequestOptions = {
			hostname: config.airbrake.host,
			port: config.airbrake.port,
			method: 'POST',
			path: requesturl,
			headers: {
				'Content-Type': "text/xml",
				'Connection': "close",
			},
		};

		var startAirbrake = microtime.now();

		// Make the real request to Airbrake
		var airbrakeRequest = config.airbrake.connection.request(airbrakeRequestOptions, function (airbrakeResponse) {
			var responseData = '';
			airbrakeResponse.on('data', function (chunk) {
				responseData += chunk;
			}).on('end', function () {
				var endAirbrake = microtime.now();
				statsd.timing(config.statsd.prefix + '.airbrake.request', ((endAirbrake - startAirbrake) / 1000));

				xmlparse(responseData, function (error, airbrake) {
					if (airbrake && airbrake.notice && airbrake.notice.id[0]) {
						redis.hset(config.redis.key, responseuuid, airbrake.notice.id[0]);
						statsd.increment(config.statsd.prefix + '.airbrake.request.success');
					} else {
						util.log("XML object returned from " + config.airbrake.host + ":" + config.airbrake.port + " is invalid; response: " + responseData);
						statsd.increment(config.statsd.prefix + '.airbrake.request.fail.xml');
					}
				});
			});
		});

		// Set a connection timeout
		airbrakeRequest.on('socket', function (socket) {
			socket.setTimeout(config.airbrake.timeout);
			socket.on('timeout', function () {
				statsd.increment(config.statsd.prefix + '.airbrake.request.fail.timeout');
				
				util.log("Connection to " + config.airbrake.host + ":" + config.airbrake.port + " for " + responseuuid + " timed out after " + config.airbrake.timeout + "ms");
				airbrakeRequest.abort();
			});
		});

		// Drop the request if we had a communication error
		airbrakeRequest.on('error', function (error) {
			util.log("Failed sending request to " + config.airbrake.host + ":" + config.airbrake.port + " for " + responseuuid + ", exception lost; error: " + error);
			airbrakeRequest.abort();
			statsd.increment(config.statsd.prefix + '.airbrake.request.fail.error');
		});

		// Send the Airbrake XML data and end the connection
		airbrakeRequest.write(data);
		airbrakeRequest.end();
	};

	// Create an HTTP server to listen to lookup GET requests and Airbrake client POST requests
	http.createServer(function (request, response) {
		var requesturl = request.url;
		var startHTTP = microtime.now();

		if (request.method === "GET") {
			requesturl = requesturl.replace(/\/locate\//g, '').replace(/\//g, '').substring(0, 36);
			redis.hget(config.redis.key, requesturl, function (error, reply) {
				if (reply) {
					response.writeHead(303, {
						'Location': 'https://airbrake.io/locate/' + reply,
					});
				} else {
					response.writeHead(404);
				}

				response.end();
				request.connection.destroy();
			});
		} else {
			var data = '';
			request.on('data', function (chunk) {
				data += chunk;
			}).on('end', function () {
				// Create a UUID and immediately return it to the client
				var responseuuid = uuid.uuid();
				response.writeHead(200);
				response.end(xmlresponse.replace(/{UUID}/g, responseuuid));
				request.connection.destroy();

				// Store stats about the request
				var endHTTP = microtime.now();
				statsd.timing(config.statsd.prefix + '.http.request', ((endHTTP - startHTTP) / 1000));

				// Store the initial UUID in Redis and make the Airbrake request
				redis.hset(config.redis.key, responseuuid, "null", function (error) {
					store(requesturl, responseuuid, data);
				});
			});
		}
	}).listen(config.listen.port, config.listen.host);
}
