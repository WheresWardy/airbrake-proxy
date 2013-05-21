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
var uuid = require('./uuid');

// Fetch the number of CPU threads and determine minimum cluster worker count
var cpus = require('os').cpus().length;
var workers = ((cpus / 2) > 1) ? Math.floor((cpus / 2)) : 1;

/*
 * Configuration
 */

// Load configuration from disk
try {
	var config = require('./config/config.json');
} catch (e) {
	util.log("Could not load configuration, did you create config.json?");
	process.exit(1);
}

// Listen process host and port settings and validation
config.listen = (config.listen == null) ? {'host': null, 'port': null} : config.listen;
config.listen.host = (config.listen.host == null) ? "0.0.0.0" : config.listen.host;
config.listen.port = (config.listen.port == null) ? 80 : parseInt(config.listen.port);
config.listen.workers = ((config.listen.workers == null) || (config.listen.workers == "cpu")) ? workers : parseInt(config.listen.workers);
config.listen.hostname = (config.listen.hostname == null) ? "localhost" : config.listen.hostname;

if ((config.listen.port < 1) || (config.listen.port > 65535) || isNaN(config.listen.port)) {
	util.log("Listen port number seems invalid, check configuration");
	process.exit(2);
}

if ((config.listen.workers == 0) || isNaN(config.listen.workers)) {
	util.log("Cluster worker count is 0, so no workers would be created");
	process.exit(3)
}

// Configure the airbrake service hostname
config.airbrake = (config.airbrake == null) ? {'host': null, 'port': null} : config.airbrake;
config.airbrake.host = (config.airbrake.host == null) ? "api.airbrake.io" : config.airbrake.host.replace(/https?:\/\//, '');
config.airbrake.port = (config.airbrake.port == null) ? 443 : parseInt(config.airbrake.port);
config.airbrake.timeout = ((config.airbrake.timeout == null) || (parseInt(config.airbrake.timeout) == 0)) ? 10000 : parseInt(config.airbrake.timeout);

if ((config.airbrake.port < 1) || (config.airbrake.port > 65535) || isNaN(config.airbrake.port)) {
	util.log("Airbrake port number seems invalid, check configuration");
	process.exit(4);
}

// Choose the protocol to connect to Airbrake to based on the port
if (config.airbrake.port == 443) {
	config.airbrake.protocol = require('https');
} else {
	config.airbrake.protocol = require('http');
}

// Redis host and port settings and validation
config.redis = (config.redis == null) ? {'host': null, 'port': null, 'prefix': null} : config.redis;
config.redis.host = (config.redis.host == null) ? "127.0.0.1" : config.redis.host;
config.redis.port = (config.redis.port == null) ? 6379 : parseInt(config.redis.port);
config.redis.prefix = (config.redis.prefix == null) ? "airbrakeproxy" : config.redis.prefix;
config.redis.key = config.redis.prefix + ":uuid";

if ((config.redis.port < 1) || (config.redis.port > 65535) || isNaN(config.redis.port)) {
	util.log("Redis port number seems invalid, check configuration");
	process.exit(5);
}

// Create Redis connection
var redis = require('redis').createClient(config.redis.port, config.redis.host, {'enable_offline_queue': false});
redis.on('error', function (error) {
	util.log("Could not create a Redis client connection to " + config.redis.host + ":" + config.redis.port);
	process.exit(6);
});

// StatsD configuration and validation
config.statsd = (config.statsd == null) ? {'host': null, 'port': null, 'prefix': null} : config.statsd;
config.statsd.host = (config.statsd.host == null) ? "127.0.0.1" : config.statsd.host;
config.statsd.port = (config.statsd.port == null) ? 8125 : parseInt(config.statsd.port);
config.statsd.prefix = (config.statsd.prefix == null) ? "airbrakeproxy" : config.statsd.prefix;

if ((config.statsd.port < 1) || (config.statsd.port > 65535) || isNaN(config.statsd.port)) {
	util.log("StatsD port number seems invalid, check configuration");
	process.exit(7);
}

// Create StatsD connection
statsd = new nodestatsd(config.statsd.host, config.statsd.port);

// Airbrake style XML response to send to client
var xmlresponse = '<?xml version="1.0"?><notice><id>{UUID}</id><url>http://' + config.listen.hostname + ':' + config.listen.port + '/locate/{UUID}</url></notice>';

/*
 * Worker cluster
 */

if (cluster.isMaster) {
	var fork = function () {
		cluster.fork();
	}

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
	var store = function (responseuuid, data) {
		var airbrakeRequestOptions = {
			hostname: config.airbrake.host,
			port: config.airbrake.port,
			method: 'POST',
			path: '/notifier_api/v2/notices',
			headers: {
				'Content-Type': "text/xml",
				'Connection': "close",
			},
		};

		var startAirbrake = microtime.now();

		// Make the real request to Airbrake
		var airbrakeRequest = config.airbrake.protocol.request(airbrakeRequestOptions, function (airbrakeResponse) {
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
	}

	// Create an HTTP server to listen to lookup GET requests and Airbrake client POST requests
	http.createServer(function (request, response) {
		var startHTTP = microtime.now();

		if (request.method == "GET") {
			requesturl = request.url.replace(/\/locate\//g, '').replace(/\//g, '').substring(0, 36);
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
			})
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
					store(responseuuid, data);
				});
			});
		}
	}).listen(config.listen.port, config.listen.host);
}
